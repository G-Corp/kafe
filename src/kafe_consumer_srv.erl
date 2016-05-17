% @hidden
-module(kafe_consumer_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

%% API.
-export([start_link/2]).

%% gen_server.
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-record(state, {
          group_id,
          generation_id = -1,
          member_id = <<>>,
          topics = [],
          fetchers = [],
          callback = undefined,
          fetch_interval = ?DEFAULT_CONSUMER_FETCH_INTERVAL,
          fetch_pids = [],
          fetch_size = ?DEFAULT_CONSUMER_FETCH_SIZE,
          max_bytes = ?DEFAULT_FETCH_MAX_BYTES,
          min_bytes = ?DEFAULT_FETCH_MIN_BYTES,
          max_wait_time = ?DEFAULT_FETCH_MAX_WAIT_TIME,
          autocommit = ?DEFAULT_CONSUMER_AUTOCOMMIT,
          allow_unordered_commit = ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT,
          commits = #{}
         }).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupID, Options) ->
  gen_server:start_link(?MODULE, [GroupID, Options], []).

%% gen_server.

% @hidden
init([GroupID, Options]) ->
  _ = erlang:process_flag(trap_exit, true),
  FetchInterval = maps:get(fetch_interval, Options, ?DEFAULT_CONSUMER_FETCH_INTERVAL),
  FetchSize = maps:get(fetch_size, Options, ?DEFAULT_CONSUMER_FETCH_SIZE),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  Autocommit = maps:get(autocommit, Options, ?DEFAULT_CONSUMER_AUTOCOMMIT),
  AllowUnorderedCommit = maps:get(allow_unordered_commit, Options, ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT),
  {ok, #state{
          group_id = bucs:to_binary(GroupID),
          callback = maps:get(callback, Options),
          fetch_interval = FetchInterval,
          fetch_size = FetchSize,
          max_bytes = MaxBytes,
          min_bytes = MinBytes,
          max_wait_time = MaxWaitTime,
          autocommit = Autocommit,
          allow_unordered_commit = AllowUnorderedCommit
         }}.

% @hidden
handle_call(describe, _From, #state{group_id = GroupID} = State) ->
  {reply, kafe:describe_group(GroupID), State};
handle_call(member_id, _From, #state{member_id = MemberID} = State) ->
  {reply, MemberID, State};
handle_call({member_id, MemberID}, _From, State) ->
  {reply, ok, State#state{member_id = MemberID}};
handle_call(generation_id, _From, #state{generation_id = GenerationID} = State) ->
  {reply, GenerationID, State};
handle_call({generation_id, GenerationID}, _From, State) ->
  {reply, ok, State#state{generation_id = GenerationID}};
handle_call(topics, _From, #state{topics = Topics} = State) ->
  {reply, Topics, State};
handle_call({topics, Topics}, _From, #state{topics = CurrentTopics} = State) ->
  if
    Topics == CurrentTopics ->
      {reply, ok, State};
    true ->
      {reply, ok, start_fetchers(Topics, State#state{topics = Topics})}
  end;
handle_call({commit, Topic, Partition, Offset}, _From, #state{allow_unordered_commit = true,
                                                              commits = Commits,
                                                              group_id = GroupID,
                                                              generation_id = GenerationID,
                                                              member_id = MemberID} = State) ->
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  CommitsList = maps:get(CommitStoreKey, Commits, []),
  case lists:keyfind({Topic, Partition, Offset}, 1, CommitsList) of
    {{Topic, Partition, Offset}, _} ->
      case commit(
             lists:keyreplace({Topic, Partition, Offset}, 1, CommitsList, {{Topic, Partition, Offset}, true}),
             ok, GroupID, GenerationID, MemberID) of
        {ok, CommitsList1} ->
          case lists:keyfind({Topic, Partition, Offset}, 1, CommitsList1) of
            {{Topic, Partition, Offset}, true} ->
              {reply, delayed, State#state{commits = maps:put(CommitStoreKey, CommitsList1, Commits)}};
            false ->
              {reply, ok, State#state{commits = maps:put(CommitStoreKey, CommitsList1, Commits)}}
          end;
        {Error, CommitsList1} ->
          {reply, Error, State#state{commits = maps:put(CommitStoreKey, CommitsList1, Commits)}}
      end;
    false ->
      {reply, {error, invalid_commit_ref}, State}
  end;
handle_call({commit, Topic, Partition, Offset}, _From, #state{allow_unordered_commit = false,
                                                              commits = Commits,
                                                              group_id = GroupID,
                                                              generation_id = GenerationID,
                                                              member_id = MemberID} = State) ->
  NoError = kafe_error:code(0),
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  case maps:get(CommitStoreKey, Commits, []) of
    [{{Topic, Partition, Offset}, _}|CommitsList] ->
      lager:debug("COMMIT Offset ~p for Topic ~p, partition ~p", [Offset, Topic, Partition]),
      case kafe:offset_commit(GroupID, GenerationID, MemberID, -1,
                              [{Topic, [{Partition, Offset, <<>>}]}]) of
        {ok, [#{name := Topic,
                partitions := [#{error_code := NoError,
                                 partition := Partition}]}]} ->
          {reply, ok, State#state{commits = maps:put(CommitStoreKey, CommitsList, Commits)}};
        {ok, [#{name := Topic,
                partitions := [#{error_code := Error,
                                 partition := Partition}]}]} ->
          {reply, {error, Error}, State};
        Error ->
          {reply, Error, State}
      end;
    _ ->
      {reply, {error, missing_previous_commit}, State}
  end;
handle_call({store_for_commit, Topic, Partition, Offset}, _From, #state{commits = Commits} = State) ->
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  CommitsList = lists:append(maps:get(CommitStoreKey, Commits, []),
                             [{{Topic, Partition, Offset}, false}]),
  lager:debug("STORE FOR COMMIT Offset ~p for Topic ~p, partition ~p", [Offset, Topic, Partition]),
  {reply,
   kafe_consumer:encode_group_commit_identifier(self(), Topic, Partition, Offset),
   State#state{commits = maps:put(CommitStoreKey, CommitsList, Commits)}};
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info({'DOWN', MonitorRef, Type, Object, Info}, State) ->
  lager:info("DOWN ~p, ~p, ~p, ~p", [MonitorRef, Type, Object, Info]),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, #state{fetchers = Fetchers} = State) ->
  _ = stop_fetchers([TP || {TP, _, _} <- Fetchers], State),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% @hidden
start_fetchers(Topics, #state{fetchers = Fetchers} = State) ->
  State1 = stop_fetchers(
             [TP || {TP, _, _} <- Fetchers] -- lists:foldl(fun({Topic, Partitions}, Acc) ->
                                                               lists:zip(
                                                                 lists:duplicate(length(Partitions), Topic),
                                                                 Partitions) ++ Acc
                                                           end, [], Topics),
             State),
  start_fetchers_for_topic(Topics, State1).

start_fetchers_for_topic([], State) ->
  State;
start_fetchers_for_topic([{Topic, Partitions}|Rest], State) ->
  start_fetchers_for_topic(Rest,
                           start_fetchers_for_topic_and_partition(Partitions, Topic, State)).

start_fetchers_for_topic_and_partition([], _, State) ->
  State;
start_fetchers_for_topic_and_partition([Partition|Partitions], Topic, #state{fetchers = Fetchers,
                                                                             fetch_interval = FetchInterval,
                                                                             group_id = GroupID,
                                                                             generation_id = GenerationID,
                                                                             member_id = MemberID,
                                                                             fetch_size = FetchSize,
                                                                             autocommit = Autocommit,
                                                                             min_bytes = MinBytes,
                                                                             max_bytes = MaxBytes,
                                                                             max_wait_time = MaxWaitTime,
                                                                             callback = Callback} = State) ->
  case lists:keyfind({Topic, Partition}, 1, Fetchers) of
    false ->
      case kafe_consumer_fetcher_sup:start_child(Topic, Partition, self(), FetchInterval,
                                                 GroupID, GenerationID, MemberID,
                                                 FetchSize, Autocommit,
                                                 MinBytes, MaxBytes, MaxWaitTime, Callback) of
        {ok, Pid} ->
          MRef = erlang:monitor(process, Pid),
          start_fetchers_for_topic_and_partition(Partitions, Topic, State#state{fetchers = [{{Topic, Partition}, Pid, MRef}|Fetchers]});
        {error, Error} ->
          lager:error("Faild to start fetcher for ~p#~p : ~p", [Topic, Partition, Error]),
          start_fetchers_for_topic_and_partition(Partitions, Topic, State)
      end;
    _ ->
      start_fetchers_for_topic_and_partition(Partitions, Topic, State)
  end.

stop_fetchers([], State) ->
  State;
stop_fetchers([TP|Rest], #state{fetchers = Fetchers, commits = Commits} = State) ->
  case lists:keyfind(TP, 1, Fetchers) of
    {TP, Pid, MRef} ->
      CommitStoreKey = erlang:term_to_binary(TP),
      _ = erlang:demonitor(MRef),
      _ = kafe_consumer_fetcher_sup:stop_child(Pid),
      stop_fetchers(Rest, State#state{fetchers = lists:keydelete(TP, 1, Fetchers),
                                      commits = maps:remove(CommitStoreKey, Commits)});
    false ->
      stop_fetchers(Rest, State)
  end.

commit([{_, false}|_] = Rest, Result, _, _, _) ->
  {Result, Rest};
commit([{{T, P, O}, true}|Rest] = All, ok, GroupID, GenerationID, MemberID) ->
  lager:debug("COMMIT Offset ~p for Topic ~p, partition ~p", [O, T, P]),
  NoError = kafe_error:code(0),
  case kafe:offset_commit(GroupID, GenerationID, MemberID, -1,
                          [{T, [{P, O, <<>>}]}]) of
    {ok, [#{name := T,
            partitions := [#{error_code := NoError,
                             partition := P}]}]} ->
      commit(Rest, ok, GroupID, GenerationID, MemberID);
    {ok, [#{name := T,
            partitions := [#{error_code := Error,
                             partition := P}]}]} ->
      {{error, Error}, All};
    Error ->
      {Error, All}
  end.

