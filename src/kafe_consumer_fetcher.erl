% @hidden
-module(kafe_consumer_fetcher).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-include("../include/kafe_consumer.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API.
-export([start_link/11]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
          topic = undefined,
          partition = undefined,
          fetch_interval = ?DEFAULT_CONSUMER_FETCH_INTERVAL,
          timer = undefined,
          offset = -1,
          group_id = undefined,
          commit = ?DEFAULT_CONSUMER_COMMIT,
          from_beginning = ?DEFAULT_CONSUMER_START_FROM_BEGINNING,
          min_bytes = ?DEFAULT_FETCH_MIN_BYTES,
          max_bytes = ?DEFAULT_FETCH_MAX_BYTES,
          max_wait_time = ?DEFAULT_FETCH_MAX_WAIT_TIME,
          callback = undefined,
          errors_actions = ?DEFAULT_CONSUMER_FETCH_ERROR_ACTIONS
         }).

start_link(Topic, Partition, FetchInterval,
           GroupID, Commit, FromBeginning,
           MinBytes, MaxBytes, MaxWaitTime,
           ErrorsActions, Callback) ->
  gen_server:start_link(?MODULE, [
                                  Topic
                                  , Partition
                                  , FetchInterval
                                  , GroupID
                                  , Commit
                                  , FromBeginning
                                  , MinBytes
                                  , MaxBytes
                                  , MaxWaitTime
                                  , ErrorsActions
                                  , Callback
                                 ], []).

%% gen_server.

init([Topic, Partition, FetchInterval,
      GroupID, Commit, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      ErrorsActions, Callback]) ->
  erlang:process_flag(trap_exit, true),
  case get_start_offset(GroupID, Topic, Partition, FromBeginning) of
    {ok, Offset} ->
      lager:info("Starting fetcher for topic ~s, partition ~p with offset ~p", [Topic, Partition, Offset]),
      CommitProcess = case lists:member(after_processing, Commit) of
                        true ->
                          after_processing;
                        false ->
                          case lists:member(before_processing, Commit) of
                            true ->
                              before_processing;
                            false ->
                              undefined
                          end
                      end,
      CommitterPID = kafe_consumer_store:value(GroupID, {commit_pid, {Topic, Partition}}),
      gen_server:call(CommitterPID, {offset, Offset}),
      {ok, #state{
              topic = Topic,
              partition = Partition,
              fetch_interval = FetchInterval,
              timer = erlang:send_after(FetchInterval, self(), fetch),
              offset = Offset,
              group_id = GroupID,
              commit = CommitProcess,
              from_beginning = FromBeginning,
              min_bytes = MinBytes,
              max_bytes = MaxBytes,
              max_wait_time = MaxWaitTime,
              callback = Callback,
              errors_actions = maps:get(fetch, ErrorsActions, ?DEFAULT_CONSUMER_FETCH_ERROR_ACTIONS)
             }};
    _ ->
      {stop, fetch_offset_faild}
  end.

handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(fetch, #state{group_id = GroupID,
                          fetch_interval = FetchInterval} = State) ->
  case fetch(State) of
    {ok, NextOffset} ->
      {noreply,
       State#state{timer = erlang:send_after(FetchInterval, self(), fetch),
                   offset = NextOffset}};
    {wait, Time} ->
      {noreply,
       State#state{timer = erlang:send_after(Time, self(), fetch)}};
    stop ->
      kafe_consumer_sup:stop_child(GroupID),
      {noreply,
       State#state{timer = erlang:send_after(FetchInterval, self(), fetch)}};
    _ ->
      {noreply,
       State#state{timer = erlang:send_after(FetchInterval, self(), fetch)}}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

terminate(Reason, State) ->
  lager:debug("Stop fetcher ~p for reason ~p", [State, Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

fetch(#state{topic = Topic,
             partition = Partition,
             offset = Offset,
             group_id = GroupID,
             commit = Commit,
             min_bytes = MinBytes,
             max_bytes = MaxBytes,
             max_wait_time = MaxWaitTime,
             callback = Callback,
             errors_actions = ErrorsActions}) ->
  case kafe_consumer:can_fetch(GroupID) of
    true ->
      case kafe:fetch(-1, [{Topic, [{Partition, Offset, MaxBytes}]}],
                      #{min_bytes => MinBytes,
                        max_wait_time => MaxWaitTime}) of
        {ok, #{topics :=
               [#{name := Topic,
                  partitions :=
                  [#{error_code := none,
                     messages := [],
                     partition := Partition}]}]}} ->
          {ok, Offset};
        {ok, #{topics :=
               [#{name := Topic,
                  partitions :=
                  [#{error_code := none,
                     messages := Messages,
                     partition := Partition}]}]}} ->
          kafe_metrics:consumer_messages(GroupID, length(Messages)),
          kafe_metrics:consumer_partition_messages(GroupID, Topic, Partition, length(Messages)),
          process_messages(Messages, Topic, Partition, Commit, GroupID, Callback, Offset, erlang:system_time(milli_seconds));
        {ok, #{topics :=
               [#{name := Topic,
                  partitions :=
                  [#{error_code := ErrorCode,
                     partition := Partition}]}]}} ->
          lager:error("Error fetching offset ~p of topic ~s, partition ~p: ~s", [Offset + 1, Topic, Partition, kafe_error:message(ErrorCode)]),
          get_error_action(ErrorCode, ErrorsActions, Topic, Partition, Offset, GroupID);
        Error ->
          lager:error("Error fetching offset ~p of topic ~s, partition ~p: ~p", [Offset + 1, Topic, Partition, Error]),
          Error
      end;
    false ->
      {ok, Offset}
  end.

get_error_action(ErrorCode, ErrorsActions, Topic, Partition, Offset, GroupID) ->
  case maps:get(ErrorCode, ErrorsActions, maps:get('*', ErrorsActions, undefined)) of
    undefined ->
      {error, ErrorCode};
    error ->
      {error, ErrorCode};
    {call, {Module, Function, Args}} ->
      try
        erlang:apply(Module, Function, [Topic, Partition, Offset, GroupID|Args])
      catch
        Class:Error ->
          lager:error("Error calling ~s:~s/~p, topic ~s, partition ~p, offset ~p, group ~s, args ~p: ~p : ~p",
                      [Module, Function, Topic, Partition, Offset, GroupID, Args, Class, Error]),
          stop
      end;
    {retry, Time} ->
      {wait, Time};
    stop ->
      stop;
    exit ->
      exit(ErrorCode);
    retry ->
      {ok, Offset};
    next ->
      commit(Offset, Topic, Partition, GroupID),
      {ok, Offset + 1};
    {reset, earliest} ->
      case get_partition_offset(Topic, Partition, -2) of
        {ok, Offset1} ->
          commit(Offset1 - 1, Topic, Partition, GroupID),
          {ok, Offset1};
        _ ->
          {error, internal_error}
      end;
    {reset, latest} ->
      case get_partition_offset(Topic, Partition, -1) of
        {ok, Offset1} ->
          commit(Offset1 - 1, Topic, Partition, GroupID),
          {ok, Offset1};
        _ ->
          {error, internal_error}
      end;
    _ ->
      {error, ErrorCode}
  end.

process_messages([], Topic, Partition, _, GroupID, _, LastOffset, Start) ->
  kafe_metrics:consumer_partition_duration(GroupID, Topic, Partition, erlang:system_time(milli_seconds) - Start),
  {ok, LastOffset + 1};
process_messages([#{offset := Offset,
                 key := Key,
                 value := Value}|Messages],
              Topic, Partition,
              Processing, GroupID,
              Callback, _LastOffset,
              Start) ->
  case before_processing(Offset, Topic, Partition, GroupID, Processing) of
    {error, Reason} ->
      lager:error("[~p] commit error for offset ~p of topic ~s, partition ~p: ~p", [Processing, Offset, Topic, Partition, Reason]),
      {error, Reason};
    _ ->
      lager:debug("Processing message ~p of topic ~s, partition ~p, offset ~p", [Offset, Topic, Partition, Offset]),
      case call_subscriber(Callback, GroupID, Topic, Partition, Offset, Key, Value) of
        ok ->
          case after_processing(Offset, Topic, Partition, GroupID, Processing) of
            {error, Reason} ->
              lager:error("[~p] commit error for offset ~p of topic ~s, patition ~p: ~p", [Processing, Offset, Topic, Partition, Reason]),
              {error, Reason};
            _ ->
              Next = erlang:system_time(milli_seconds),
              kafe_metrics:consumer_partition_duration(GroupID, Topic, Partition, Next - Start),
              process_messages(Messages, Topic, Partition, Processing, GroupID, Callback, Offset, Next)
          end;
        callback_exception ->
          {error, callback_exception};
        {error, Reason1} ->
          lager:error("Callback for message ~p of topic ~s, partition ~p returned error: ~p", [Offset, Topic, Partition, Reason1]),
          {error, Reason1};
        Other ->
          lager:error("Callback for message ~p of topic ~s, partition ~p returned invalid response: ~p", [Offset, Topic, Partition, Other]),
          {error, Other}
      end
  end.

call_subscriber(Callback, GroupID, Topic, Partition, Offset, Key, Value) when is_function(Callback, 6) ->
  try
    erlang:apply(Callback, [GroupID, Topic, Partition, Offset, Key, Value])
  catch
    Class:Reason0 ->
      lager:error(
        "Callback for message ~p of topic ~s, partition ~p crash:~s",
        [Offset, Topic, Partition, lager:pr_stacktrace(erlang:get_stacktrace(), {Class, Reason0})]),
      callback_exception
  end;
call_subscriber(Callback, GroupID, Topic, Partition, Offset, Key, Value) when is_function(Callback, 1) ->
  try
    erlang:apply(Callback, [#message{
                               group_id = GroupID,
                               topic = Topic,
                               partition = Partition,
                               offset = Offset,
                               key = Key,
                               value = Value}])
  catch
    Class:Reason0 ->
      lager:error(
        "Callback for message ~p of topic ~s, partition ~p crash:~s",
        [Offset, Topic, Partition, lager:pr_stacktrace(erlang:get_stacktrace(), {Class, Reason0})]),
      callback_exception
  end;
call_subscriber(Callback, GroupID, Topic, Partition, Offset, Key, Value) when is_atom(Callback);
                                                                              is_tuple(Callback) ->
  SubscriberPID = kafe_consumer_store:value(GroupID, {subscriber_pid, {Topic, Partition}}),
  gen_server:call(SubscriberPID, {message, GroupID, Topic, Partition, Offset, Key, Value}, infinity).


before_processing(Offset, Topic, Partition, GroupID, before_processing) ->
  commit(Offset, Topic, Partition, GroupID);
before_processing(_, _, _, _, _) ->
  ok.

after_processing(Offset, Topic, Partition, GroupID, after_processing) ->
  commit(Offset, Topic, Partition, GroupID);
after_processing(_, _, _, _, _) ->
  ok.

commit(Offset, Topic, Partition, GroupID) ->
  kafe_consumer:commit(GroupID, Topic, Partition, Offset).

get_start_offset(GroupID, Topic, Partition, FromBeginning) ->
  case kafe:offset_fetch(GroupID, [{Topic, [Partition]}]) of
    {ok, [#{name := Topic,
            partitions_offset := [#{error_code := none,
                                    offset := Offset,
                                    partition := Partition}]}]} ->
      case (Offset < 0) of
        true ->
          Time = case FromBeginning of
                   true -> -2;
                   false -> -1
                 end,
          get_partition_offset(Topic, Partition, Time);
        false ->
          {ok, Offset}
      end;
    _ ->
      error
  end.

get_partition_offset(Topic, Partition, Time) ->
  case kafe:offset(-1, [{Topic, [{Partition, Time, 1}]}]) of
    {ok, [#{name := Topic,
            partitions := [#{error_code := none,
                             id := Partition,
                             offsets := [Offset]}]}]} ->
      {ok, Offset};
    {ok, [#{name := Topic,
            partitions := [#{error_code := none,
                             id := Partition,
                             offset := Offset}]}]} when is_integer(Offset) ->
      {ok, Offset};
    _ ->
      error
  end.

-ifdef(TEST).
fetch_without_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, [{Topic, [{Partition, Offset, _}]}], _) ->
                               {ok, #{topics =>
                                      [#{name => Topic,
                                         partitions =>
                                         [#{error_code => none,
                                            high_watermark_offset => Offset + 2,
                                            messages => [
                                                         #{offset => Offset,
                                                           key => <<"key100">>,
                                                           value => <<"value100">>},
                                                         #{offset => Offset + 1,
                                                           key => <<"key101">>,
                                                           value => <<"value101">>}
                                                        ],
                                            partition => Partition}]}]}}
                           end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_messages, 2, ok),
  meck:expect(kafe_metrics, consumer_partition_messages, 4, ok),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),

  ?assertMatch({ok, 101},
               fetch(#state{
                        topic = <<"topic">>,
                        partition = 10,
                        fetch_interval = 100,
                        % timer
                        offset = 99,
                        group_id = <<"group">>,
                        commit = undefined,
                        % from_beginning
                        min_bytes = 1,
                        max_bytes = 10000,
                        max_wait_time = 10,
                        callback = fun(_, _, _, _, _, _) -> ok end
                       })),

  meck:unload(kafe_metrics),
  meck:unload(kafe),
  meck:unload(kafe_consumer).

can_not_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> false end),

  ?assertMatch({ok, 99},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            commit = after_processing,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe_consumer).

kafka_fetch_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, fetch, fun(_, [{Topic, [{Partition, _Offset, _}]}], _) ->
                               {ok, #{topics =>
                                      [#{name => Topic,
                                         partitions =>
                                         [#{error_code => unknown_topic_or_partition,
                                            partition => Partition}]}]}}
                           end),

  ?assertMatch({error, unknown_topic_or_partition},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            commit = undefined,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

kafka_fetch_offset_out_of_range_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, fetch, fun(_, [{Topic, [{Partition, _Offset, _}]}], _) ->
                               {ok, #{topics =>
                                      [#{name => Topic,
                                         partitions =>
                                         [#{error_code => offset_out_of_range,
                                            high_watermark_offset => -1,
                                            partition => Partition}]}]}}
                           end),

  ?assertMatch({error, offset_out_of_range},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            commit = undefined,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

fetch_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, fetch, fun(_, _, _) ->
                               {error, test_error}
                           end),

  ?assertMatch({error, test_error},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            commit = undefined,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

process_messages_without_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual({ok, 101},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual({ok, 103},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>},
                              #{offset => 102, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual({ok, 101},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual({ok, 103},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>},
                              #{offset => 102, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

process_messages_with_invalid_processing_commit_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual({ok, 102},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             invalid_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

process_messages_with_commit_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, {error, test_error}),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual({error, test_error},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual({error, test_error},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

process_messages_with_callback_exception_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual({error, callback_exception},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, <<"bad match">>) -> ok end,
                             100,
                             0)),
  ?assertEqual({error, callback_exception},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, <<"bat match">>) -> ok end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

process_messages_with_callback_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual({error, test_error},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> {error, test_error} end,
                             100,
                             0)),
  ?assertEqual({error, test_error},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> {error, test_error} end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

process_messages_with_invalid_callback_response_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual({error, invalid_test_response},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> invalid_test_response end,
                             100,
                             0)),
  ?assertEqual({error, invalid_test_response},
               process_messages([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> invalid_test_response end,
                             100,
                             0)),
  meck:unload(kafe_metrics),
  meck:unload(kafe_consumer).

commit_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, commit_result),

  ?assertEqual(commit_result,
               before_processing(offset, topic, partition, group, before_processing)),
  ?assertEqual(commit_result,
               after_processing(offset, topic, partition, group, after_processing)),
  ?assertEqual(ok,
               before_processing(offset, topic, partition, group, after_processing)),
  ?assertEqual(ok,
               after_processing(offset, topic, partition, group, before_processing)),

  meck:unload(kafe_consumer).
-endif.

