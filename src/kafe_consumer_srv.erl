% @hidden
-module(kafe_consumer_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
          topics = [],
          fetchers = [],
          callback = undefined,
          fetch_interval = ?DEFAULT_CONSUMER_FETCH_INTERVAL,
          fetch_pids = [],
          max_bytes = ?DEFAULT_FETCH_MAX_BYTES,
          min_bytes = ?DEFAULT_FETCH_MIN_BYTES,
          max_wait_time = ?DEFAULT_FETCH_MAX_WAIT_TIME,
          autocommit = ?DEFAULT_CONSUMER_AUTOCOMMIT,
          allow_unordered_commit = ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT,
          commits = #{},
          processing = ?DEFAULT_CONSUMER_PROCESSING,
          on_start_fetching = ?DEFAULT_CONSUMER_ON_START_FETCHING,
          on_stop_fetching = ?DEFAULT_CONSUMER_ON_STOP_FETCHING,
          on_assignment_change = ?DEFAULT_CONSUMER_ON_ASSIGNMENT_CHANGE
         }).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupID, Options) ->
  gen_server:start_link(?MODULE, [GroupID, Options], []).

%% gen_server.

% @hidden
init([GroupID, Options]) ->
  erlang:process_flag(trap_exit, true),
  kafe_consumer_store:insert(GroupID, server_pid, self()),
  kafe_consumer_store:insert(GroupID, can_fetch, false),
  AllowUnorderedCommit = maps:get(allow_unordered_commit, Options, ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT),
  kafe_consumer_store:insert(GroupID, allow_unordered_commit, AllowUnorderedCommit),
  FetchInterval = maps:get(fetch_interval, Options, ?DEFAULT_CONSUMER_FETCH_INTERVAL),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  Autocommit = maps:get(autocommit, Options, ?DEFAULT_CONSUMER_AUTOCOMMIT),
  Processing = maps:get(processing, Options, ?DEFAULT_CONSUMER_PROCESSING),
  OnStartFetching = maps:get(on_start_fetching, Options, ?DEFAULT_CONSUMER_ON_START_FETCHING),
  OnStopFetching = maps:get(on_stop_fetching, Options, ?DEFAULT_CONSUMER_ON_STOP_FETCHING),
  OnAssignmentChange = maps:get(on_assignment_change, Options, ?DEFAULT_CONSUMER_ON_ASSIGNMENT_CHANGE),
  {ok, #state{
          group_id = bucs:to_binary(GroupID),
          callback = maps:get(callback, Options),
          fetch_interval = FetchInterval,
          max_bytes = MaxBytes,
          min_bytes = MinBytes,
          max_wait_time = MaxWaitTime,
          autocommit = Autocommit,
          allow_unordered_commit = AllowUnorderedCommit,
          processing = Processing,
          on_start_fetching = OnStartFetching,
          on_stop_fetching = OnStopFetching,
          on_assignment_change = OnAssignmentChange
         }}.

% @hidden
handle_call({topics, Topics}, _From, #state{topics = CurrentTopics, group_id = GroupID} = State) ->
  if
    Topics == CurrentTopics ->
      {reply, ok, State};
    true ->
      kafe_consumer_store:insert(GroupID, topics, Topics),
      {reply, ok, update_fetchers(Topics, State#state{topics = Topics})}
  end;
handle_call(start_fetch, _From, #state{group_id = GroupID, on_start_fetching = OnStartFetching} = State) ->
  case kafe_consumer_store:lookup(GroupID, can_fetch) of
    {ok, true} ->
      ok;
    _ ->
      kafe_consumer_store:insert(GroupID, can_fetch, true),
      case OnStartFetching of
        Fun when is_function(Fun, 1) ->
          _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID]) end);
        _ ->
          ok
      end
  end,
  {reply, ok, State};
handle_call(stop_fetch, _From, #state{group_id = GroupID, on_stop_fetching = OnStopFetching} = State) ->
  case kafe_consumer_store:lookup(GroupID, can_fetch) of
    {ok, true} ->
      kafe_consumer_store:insert(GroupID, can_fetch, false),
      case OnStopFetching of
        Fun when is_function(Fun, 1) ->
          _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID]) end);
        _ ->
          ok
      end;
    _ ->
      ok
  end,
  {reply, ok, State};
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
terminate(Reason, #state{group_id = GroupID, fetchers = Fetchers} = State) ->
  lager:debug("Will stop fetchers : ~p~nStacktrace:~s", [Reason, lager:pr_stacktrace(erlang:get_stacktrace())]),
  stop_fetchers([TP || {TP, _, _} <- Fetchers], State),
  kafe_consumer_store:delete(GroupID, server_pid),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% @hidden
update_fetchers(Topics, #state{fetchers = Fetchers,
                               group_id = GroupID,
                               on_assignment_change = OnAssignmentChange} = State) ->
  CurrentFetchers = [TP || {TP, _, _} <- Fetchers],
  NewFetchers = lists:foldl(fun({Topic, Partitions}, Acc) ->
                                lists:zip(
                                  lists:duplicate(length(Partitions), Topic),
                                  Partitions) ++ Acc
                            end, [], Topics),
  FetchersToStop = CurrentFetchers -- NewFetchers,
  FetchersToSart = NewFetchers -- CurrentFetchers,
  lager:debug("CurrentFetchers = ~p", [CurrentFetchers]),
  lager:debug("NewFetchers     = ~p", [NewFetchers]),
  lager:debug("Stop            = ~p", [FetchersToStop]),
  lager:debug("Start           = ~p", [FetchersToSart]),
  case OnAssignmentChange of
    Fun when is_function(Fun, 3) ->
      _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID, FetchersToStop, FetchersToSart]) end);
    _ ->
      ok
  end,
  State1 = stop_fetchers(FetchersToStop, State),
  start_fetchers(FetchersToSart, State1).

stop_fetchers([], State) ->
  State;
stop_fetchers([TP|Rest], #state{fetchers = Fetchers, commits = Commits} = State) ->
  case lists:keyfind(TP, 1, Fetchers) of
    {TP, Pid, MRef} ->
      CommitStoreKey = erlang:term_to_binary(TP),
      _ = erlang:demonitor(MRef),
      try
        kafe_consumer_fetcher_sup:stop_child(Pid)
      catch
        C:E ->
          lager:error("Can't terminate kafe_consumer_fetcher #~p: ~p:~p", [Pid, C, E])
      end,
      stop_fetchers(Rest, State#state{fetchers = lists:keydelete(TP, 1, Fetchers),
                                      commits = maps:remove(CommitStoreKey, Commits)});
    false ->
      stop_fetchers(Rest, State)
  end.

start_fetchers([], State) ->
  State;
start_fetchers([{Topic, Partition}|Rest], #state{fetchers = Fetchers,
                                                 fetch_interval = FetchInterval,
                                                 group_id = GroupID,
                                                 autocommit = Autocommit,
                                                 min_bytes = MinBytes,
                                                 max_bytes = MaxBytes,
                                                 max_wait_time = MaxWaitTime,
                                                 callback = Callback,
                                                 processing = Processing} = State) ->
  case kafe_consumer_fetcher_sup:start_child(Topic, Partition, FetchInterval,
                                             GroupID, Autocommit, MinBytes, MaxBytes,
                                             MaxWaitTime, Callback, Processing) of
    {ok, Pid} ->
      MRef = erlang:monitor(process, Pid),
      start_fetchers(Rest, State#state{fetchers = [{{Topic, Partition}, Pid, MRef}|Fetchers]});
    {error, Error} ->
      lager:error("Faild to start fetcher for ~p#~p : ~p", [Topic, Partition, Error]),
      start_fetchers(Rest, State)
  end.

-ifdef(TEST).
-endif.

