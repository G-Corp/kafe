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
          commits = #{},
          processing = ?DEFAULT_CONSUMER_PROCESSING,
          fetch = false,
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
  _ = erlang:process_flag(trap_exit, true),
  FetchInterval = maps:get(fetch_interval, Options, ?DEFAULT_CONSUMER_FETCH_INTERVAL),
  FetchSize = maps:get(fetch_size, Options, ?DEFAULT_CONSUMER_FETCH_SIZE),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  Autocommit = maps:get(autocommit, Options, ?DEFAULT_CONSUMER_AUTOCOMMIT),
  AllowUnorderedCommit = maps:get(allow_unordered_commit, Options, ?DEFAULT_CONSUMER_ALLOW_UNORDERED_COMMIT),
  Processing = maps:get(processing, Options, ?DEFAULT_CONSUMER_PROCESSING),
  OnStartFetching = maps:get(on_start_fetching, Options, ?DEFAULT_CONSUMER_ON_START_FETCHING),
  OnStopFetching = maps:get(on_stop_fetching, Options, ?DEFAULT_CONSUMER_ON_STOP_FETCHING),
  OnAssignmentChange = maps:get(on_assignment_change, Options, ?DEFAULT_CONSUMER_ON_ASSIGNMENT_CHANGE),
  {ok, #state{
          group_id = bucs:to_binary(GroupID),
          callback = maps:get(callback, Options),
          fetch_interval = FetchInterval,
          fetch_size = FetchSize,
          max_bytes = MaxBytes,
          min_bytes = MinBytes,
          max_wait_time = MaxWaitTime,
          autocommit = Autocommit,
          allow_unordered_commit = AllowUnorderedCommit,
          processing = Processing,
          fetch = false,
          on_start_fetching = OnStartFetching,
          on_stop_fetching = OnStopFetching,
          on_assignment_change = OnAssignmentChange
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
      {reply, ok, update_fetchers(Topics, State#state{topics = Topics})}
  end;
handle_call({commit, Topic, Partition, Offset, Options}, _From, #state{allow_unordered_commit = true,
                                                                       commits = Commits,
                                                                       group_id = GroupID,
                                                                       generation_id = GenerationID,
                                                                       member_id = MemberID} = State) ->
  Retry = maps:get(retry, Options, ?DEFAULT_CONSUMER_COMMIT_RETRY),
  Delay = maps:get(delay, Options, ?DEFAULT_CONSUMER_COMMIT_DELAY),
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  CommitsList = maps:get(CommitStoreKey, Commits, []),
  case lists:keyfind({Topic, Partition, Offset}, 1, CommitsList) of
    {{Topic, Partition, Offset}, _} ->
      case commit(
             lists:keyreplace({Topic, Partition, Offset}, 1, CommitsList, {{Topic, Partition, Offset}, true}),
             ok, GroupID, GenerationID, MemberID, Retry, Delay) of
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
handle_call({commit, Topic, Partition, Offset, Options}, _From, #state{allow_unordered_commit = false,
                                                                       commits = Commits,
                                                                       group_id = GroupID,
                                                                       generation_id = GenerationID,
                                                                       member_id = MemberID} = State) ->
  Retry = maps:get(retry, Options, ?DEFAULT_CONSUMER_COMMIT_RETRY),
  Delay = maps:get(delay, Options, ?DEFAULT_CONSUMER_COMMIT_DELAY),
  NoError = kafe_error:code(0),
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  case maps:get(CommitStoreKey, Commits, []) of
    [{{Topic, Partition, Offset}, _}|CommitsList] ->
      lager:debug("COMMIT Offset ~p for Topic ~p, partition ~p", [Offset, Topic, Partition]),
      case do_commit(GroupID, GenerationID, MemberID,
                     Topic, Partition, Offset,
                     Retry, Delay, {error, invalid_retry}) of
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
handle_call(remove_commits, _From, State) ->
  {reply, ok, State#state{commits = #{}}};
handle_call({remove_commit, Topic, Partition, Offset}, _From, #state{commits = Commits} = State) ->
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  case maps:get(CommitStoreKey, Commits, []) of
    [{{Topic, Partition, Offset}, _}|Rest] ->
      {reply, ok, State#state{commits = maps:put(CommitStoreKey, Rest, Commits)}};
    _ ->
      {reply, {error, not_head_commit}, State}
  end;
handle_call({pending_commits, Topics}, _From, #state{commits = Commits} = State) ->
  Pendings = lists:foldl(fun(TP, Acc) ->
                             lists:append(
                               Acc,
                               [kafe_consumer:encode_group_commit_identifier(self(), T, P, O)
                                || {{T, P, O}, _} <- maps:get(erlang:term_to_binary(TP), Commits, [])])
                         end, [], Topics),
  {reply, Pendings, State};
handle_call(start_fetch, _From, #state{fetch = true} = State) ->
  {reply, ok, State};
handle_call(start_fetch, _From, #state{fetch = false, group_id = GroupID, on_start_fetching = OnStartFetching} = State) ->
  case OnStartFetching of
    Fun when is_function(Fun, 1) ->
      _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID]) end);
    _ ->
      ok
  end,
  {reply, ok, State#state{fetch = true}};
handle_call(stop_fetch, _From, #state{fetch = false} = State) ->
  {reply, ok, State};
handle_call(stop_fetch, _From, #state{fetch = true, group_id = GroupID, on_stop_fetching = OnStopFetching} = State) ->
  case OnStopFetching of
    Fun when is_function(Fun, 1) ->
      _ = erlang:spawn(fun() -> erlang:apply(Fun, [GroupID]) end);
    _ ->
      ok
  end,
  {reply, ok, State#state{fetch = false}};
handle_call(can_fetch, _From, #state{fetch = Fetch} = State) ->
  {reply, Fetch, State};
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
      _ = kafe_consumer_fetcher_sup:stop_child(Pid),
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
                                                 generation_id = GenerationID,
                                                 member_id = MemberID,
                                                 fetch_size = FetchSize,
                                                 autocommit = Autocommit,
                                                 min_bytes = MinBytes,
                                                 max_bytes = MaxBytes,
                                                 max_wait_time = MaxWaitTime,
                                                 callback = Callback,
                                                 processing = Processing} = State) ->
  case kafe_consumer_fetcher_sup:start_child(Topic, Partition, self(), FetchInterval,
                                             GroupID, GenerationID, MemberID,
                                             FetchSize, Autocommit,
                                             MinBytes, MaxBytes, MaxWaitTime,
                                             Callback, Processing) of
    {ok, Pid} ->
      MRef = erlang:monitor(process, Pid),
      start_fetchers(Rest, State#state{fetchers = [{{Topic, Partition}, Pid, MRef}|Fetchers]});
    {error, Error} ->
      lager:error("Faild to start fetcher for ~p#~p : ~p", [Topic, Partition, Error]),
      start_fetchers(Rest, State)
  end.

commit([], Result, _, _, _, _, _) ->
  {Result, []};
commit([{_, false}|_] = Rest, Result, _, _, _, _, _) ->
  {Result, Rest};
commit([{{T, P, O}, true}|Rest] = All, ok, GroupID, GenerationID, MemberID, Retry, Delay) ->
  lager:debug("COMMIT Offset ~p for Topic ~p, partition ~p", [O, T, P]),
  NoError = kafe_error:code(0),
  case do_commit(GroupID, GenerationID, MemberID,
                 T, P, O,
                 Retry, Delay, {error, invalid_retry}) of
    {ok, [#{name := T,
            partitions := [#{error_code := NoError,
                             partition := P}]}]} ->
      commit(Rest, ok, GroupID, GenerationID, MemberID, Retry, Delay);
    {ok, [#{name := T,
            partitions := [#{error_code := Error,
                             partition := P}]}]} ->
      {{error, Error}, All};
    Error ->
      {Error, All}
  end.

do_commit(_, _, _, _, _, _, Retry, _, Return) when Retry < 0 ->
  Return;
do_commit(GroupID, GenerationID, MemberID, Topic, Partition, Offset, Retry, Delay, _) ->
  NoError = kafe_error:code(0),
  case kafe:offset_commit(GroupID, GenerationID, MemberID, -1,
                          [{Topic, [{Partition, Offset, <<>>}]}]) of
    {ok, [#{name := Topic,
            partitions := [#{error_code := NoError,
                             partition := Partition}]}]} = R ->
      R;
    Other ->
      _ = timer:sleep(Delay),
      do_commit(GroupID, GenerationID, MemberID, Topic, Partition, Offset, Retry - 1, Delay, Other)
  end.

-ifdef(TEST).
commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  State0 = #state{allow_unordered_commit = false,
                 group_id = <<"FakeGroup">>,
                 generation_id = 1,
                 member_id = <<"memberID">>},
  {reply, CommitID1, State1} = handle_call({store_for_commit, <<"topic1">>, 0, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 0),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, <<"topic2">>, 0, 0}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic2">>, 0, 0),
     CommitID2),
  {reply, Commits0, State3} = handle_call({pending_commits, [{<<"topic1">>, 0}, {<<"topic2">>, 0}]}, from, State2),
  ?assertEqual(
     [kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 0),
      kafe_consumer:encode_group_commit_identifier(self(), <<"topic2">>, 0, 0)],
     Commits0),
  {reply, ok, State4} = handle_call({commit, <<"topic1">>, 0, 0, #{}}, from, State3),
  {reply, Commits1, _} = handle_call({pending_commits, [{<<"topic1">>, 0}, {<<"topic2">>, 0}]}, from, State4),
  ?assertEqual(
     [kafe_consumer:encode_group_commit_identifier(self(), <<"topic2">>, 0, 0)],
     Commits1),
  meck:unload(kafe).

delayed_commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  State0 = #state{allow_unordered_commit = true,
                 group_id = <<"FakeGroup">>,
                 generation_id = 1,
                 member_id = <<"memberID">>},
  {reply, CommitID1, State1} = handle_call({store_for_commit, <<"topic1">>, 0, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 0),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, <<"topic1">>, 0, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 1),
     CommitID2),
  {reply, Commits, State3} = handle_call({pending_commits, [{<<"topic1">>, 0}]}, from, State2),
  ?assertEqual(
     [CommitID1, CommitID2],
     Commits),
  {reply, delayed, State4} = handle_call({commit, <<"topic1">>, 0, 1, #{}}, from, State3),
  {reply, ok, State5} = handle_call({commit, <<"topic1">>, 0, 0, #{}}, from, State4),
  ?assertMatch({reply, [], _},
               handle_call({pending_commits, [{<<"topic1">>, 0}]}, from, State5)),
  meck:unload(kafe).

remove_commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  State0 = #state{allow_unordered_commit = false,
                 group_id = <<"FakeGroup">>,
                 generation_id = 1,
                 member_id = <<"memberID">>},
  {reply, CommitID1, State1} = handle_call({store_for_commit, <<"topic1">>, 0, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 0),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, <<"topic1">>, 0, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 1),
     CommitID2),
  {reply, Commits0, State3} = handle_call({pending_commits, [{<<"topic1">>, 0}]}, from, State2),
  ?assertEqual(
     [CommitID1, CommitID2],
     Commits0),
  ?assertEqual({reply, {error, not_head_commit}, State3},
               handle_call({remove_commit, <<"topic1">>, 0, 1}, from, State3)),
  {reply, ok, State4} = handle_call({remove_commit, <<"topic1">>, 0, 0}, from, State3),
  {reply, Commits1, State4} = handle_call({pending_commits, [{<<"topic1">>, 0}]}, from, State4),
  ?assertEqual(
     [CommitID2],
     Commits1),
  meck:unload(kafe).

invalid_commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  State0 = #state{allow_unordered_commit = false,
                 group_id = <<"FakeGroup">>,
                 generation_id = 1,
                 member_id = <<"memberID">>},
  {reply, CommitID1, State1} = handle_call({store_for_commit, <<"topic1">>, 0, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 0),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, <<"topic1">>, 0, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic1">>, 0, 1),
     CommitID2),
  {reply, Commits, State3} = handle_call({pending_commits, [{<<"topic1">>, 0}]}, from, State2),
  ?assertEqual(
     [CommitID1, CommitID2],
     Commits),
  ?assertMatch({reply, {error, missing_previous_commit}, _},
               handle_call({commit, <<"topic1">>, 0, 1, #{}}, from, State3)),
  {reply, ok, State4} = handle_call(remove_commits, from, State3),
  ?assertMatch({reply, [], _},
               handle_call({pending_commits, [{<<"topic1">>, 0}]}, from, State4)),
  meck:unload(kafe).

-endif.
