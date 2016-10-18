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
-export([start_link/10]).

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
          callback = undefined
         }).

start_link(Topic, Partition, FetchInterval,
           GroupID, Commit, FromBeginning,
           MinBytes, MaxBytes, MaxWaitTime,
           Callback) ->
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
                                  , Callback
                                 ], []).

%% gen_server.

init([Topic, Partition, FetchInterval,
      GroupID, Commit, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      Callback]) ->
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
      CommiterPID = kafe_consumer_store:value(GroupID, {commit_pid, {Topic, Partition}}),
      gen_server:call(CommiterPID, {offset, Offset}),
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
              callback = Callback
             }};
    _ ->
      lager:error("Failed to fetch offset for topic ~s, partition ~p in group ~s", [Topic, Partition, GroupID]),
      {stop, fetch_offset_faild}
  end.

handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(fetch, State) ->
  {noreply, fetch(State)};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(Reason, State) ->
  lager:debug("Stop fetcher ~p for reason ~p", [State, Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

fetch(#state{fetch_interval = FetchInterval,
             topic = Topic,
             partition = Partition,
             offset = Offset,
             group_id = GroupID,
             commit = Commit,
             min_bytes = MinBytes,
             max_bytes = MaxBytes,
             max_wait_time = MaxWaitTime,
             callback = Callback} = State) ->
  OffsetFetch = case kafe_consumer:can_fetch(GroupID) of
                  true ->
                    case kafe:offset([{Topic, [{Partition, -1, 1}]}]) of
                      {ok, [#{name := Topic,
                              partitions := [#{error_code := none,
                                               id := Partition,
                                               offsets := [MaxOffset]}]}]} ->
                        case (Offset + 1 =< MaxOffset - 1) of
                          true ->
                            OffsetOutOfRange = kafe_error:code(1),
                            case kafe:fetch(-1, Topic, #{partition => Partition,
                                                         offset => Offset + 1,
                                                         max_bytes => MaxBytes,
                                                         min_bytes => MinBytes,
                                                         max_wait_time => MaxWaitTime}) of
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           messages := Messages,
                                           partition := Partition}]}]}} when ErrorCode == none ->
                                kafe_metrics:consumer_messages(GroupID, length(Messages)),
                                kafe_metrics:consumer_partition_messages(GroupID, Topic, Partition, length(Messages)),
                                perform_fetch(Messages, Topic, Partition, Commit, GroupID, Callback, Offset, erlang:system_time(milli_seconds));
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           high_watermark_offset := -1,
                                           partition := Partition}]}]}} when ErrorCode == OffsetOutOfRange ->
                                % REMARK: this must never append...
                                Offset;
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           partition := Partition}]}]}} when ErrorCode == OffsetOutOfRange ->
                                Offset + 1; % TODO verify if we still have messages
                              {ok, #{topics :=
                                     [#{name := Topic,
                                        partitions :=
                                        [#{error_code := ErrorCode,
                                           partition := Partition}]}]}} ->
                                lager:error("Error fetching offset ~p of topic ~s, partition ~p: ~s", [Offset + 1, Topic, Partition, kafe_error:message(ErrorCode)]),
                                Offset;
                              Error ->
                                lager:error("Error fetching offset ~p of topic ~s, partition ~p: ~p", [Offset + 1, Topic, Partition, Error]),
                                Offset
                            end;
                          false ->
                            Offset
                        end;
                      {ok, [#{name := Topic,
                              partitions := [#{error_code := Error}]}]} ->
                        lager:error("Error getting offset for topic ~s, partition ~p: ~s", [Topic, Partition, kafe_error:message(Error)]),
                        Offset;
                      {error, Error} ->
                        lager:error("Error getting offset for topic ~s, partition ~p: ~p", [Topic, Partition, Error]),
                        Offset
                    end;
                  false ->
                    Offset
                end,
  State#state{timer = erlang:send_after(FetchInterval, self(), fetch),
              offset = OffsetFetch}.

perform_fetch([], Topic, Partition, _, GroupID, _, LastOffset, Start) ->
  kafe_metrics:consumer_partition_duration(GroupID, Topic, Partition, erlang:system_time(milli_seconds) - Start),
  LastOffset;
perform_fetch([#{offset := Offset,
                 key := Key,
                 value := Value}|Messages],
              Topic, Partition,
              Commit, GroupID,
              Callback, LastOffset,
              Start) ->
  case commit(Offset, Topic, Partition, GroupID, Commit, before_processing) of
    {error, Reason} ->
      lager:error("[~p] commit error for offset ~p of topic ~s, partition ~p: ~p", [Commit, Offset, Topic, Partition, Reason]),
      LastOffset;
    _ ->
      lager:debug("Processing message ~p of topic ~s, partition ~p, offset ~p", [Offset, Topic, Partition, Offset]),
      case call_subscriber(Callback, GroupID, Topic, Partition, Offset, Key, Value) of
        ok ->
          case commit(Offset, Topic, Partition, GroupID, Commit, after_processing) of
            {error, Reason} ->
              lager:error("[~p] commit error for offset ~p of topic ~s, patition ~p: ~p", [Commit, Offset, Topic, Partition, Reason]),
              LastOffset;
            _ ->
              Next = erlang:system_time(milli_seconds),
              kafe_metrics:consumer_partition_duration(GroupID, Topic, Partition, Next - Start),
              perform_fetch(Messages, Topic, Partition, Commit, GroupID, Callback, Offset, Next)
          end;
        callback_exception ->
          LastOffset;
        {error, Error} ->
          lager:error("Callback for message ~p of topic ~s, partition ~p returned error: ~p", [Offset, Topic, Partition, Error]),
          LastOffset;
        Other ->
          lager:error("Callback for message ~p of topic ~s, partition ~p returned invalid response: ~p", [Offset, Topic, Partition, Other]),
          LastOffset
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
  gen_server:call(SubscriberPID, {message, GroupID, Topic, Partition, Offset, Key, Value}).

commit(Offset, Topic, Partition, GroupID, Processing, Processing) when Processing == before_processing;
                                                                       Processing == after_processing ->
  kafe_consumer:commit(GroupID, Topic, Partition, Offset);
commit(_, _, _, _, Processing1, Processing2) when (Processing1 == before_processing orelse
                                                   Processing1 == after_processing orelse
                                                   Processing1 == undefined)
                                                  andalso
                                                  (Processing2 == before_processing orelse
                                                   Processing2 == after_processing) ->
  ok;
commit(_, _, _, _, _, _) ->
  {error, invalid_processing}.

get_start_offset(_, Topic, Partition, false) ->
  case kafe:offset(-1, [{Topic, [{Partition, -1, 1}]}]) of
    {ok, [#{name := Topic,
            partitions := [#{error_code := none,
                             id := Partition,
                             offsets := [Offset]}]}]} ->
      case (Offset < 0) of
        true ->
          {ok, Offset};
        false ->
          {ok, Offset - 1}
      end;
    _ ->
      error
  end;
get_start_offset(GroupID, Topic, Partition, true) ->
  case kafe:offset_fetch(GroupID, [{Topic, [Partition]}]) of
    {ok, [#{name := Topic,
            partitions_offset := [#{error_code := none,
                                    offset := Offset,
                                    partition := Partition}]}]} ->
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
  meck:expect(kafe, fetch, fun(_, Topic, #{partition := Partition,
                                           offset := Offset}) ->
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

  ?assertMatch(#state{
                  topic = <<"topic">>,
                  partition = 10,
                  fetch_interval = 100,
                  timer = _,
                  offset = 101,
                  group_id = <<"group">>,
                  commit = undefined,
                  % from_beginning
                  min_bytes = 1,
                  max_bytes = 10000,
                  max_wait_time = 10,
                  callback = _
                 },
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

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = after_processing,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
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

nothing_to_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [100]}]}]}
                            end),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_messages, 2, ok),
  meck:expect(kafe_metrics, consumer_partition_messages, 4, ok),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = after_processing,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            commit = after_processing,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe_metrics),
  meck:unload(kafe),
  meck:unload(kafe_consumer).

kafka_offset_error_on_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{_Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => unknown_topic_or_partition}]}]}
                            end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = after_processing,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
               fetch(#state{fetch_interval = 100,
                            topic = <<"topic">>,
                            partition = 10,
                            offset = 99,
                            commit = after_processing,
                            min_bytes = 1,
                            max_bytes = 10000,
                            max_wait_time = 10,
                            callback = fun(_, _, _, _, _, _) -> ok end})),

  meck:unload(kafe),
  meck:unload(kafe_consumer).

offset_error_on_fetch_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{_Topic, [{_Partition, -1, 1}]}]) ->
                                {error, test_error}
                            end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = undefined,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
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

kafka_fetch_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, can_fetch, fun(_) -> true end),
  meck:new(kafe),
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, Topic, #{partition := Partition,
                                           offset := _Offset}) ->
                              {ok, #{topics =>
                                     [#{name => Topic,
                                        partitions =>
                                        [#{error_code => unknown_topic_or_partition,
                                           partition => Partition}]}]}}
                           end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = undefined,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
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
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, Topic, #{partition := Partition,
                                           offset := _Offset}) ->
                              {ok, #{topics =>
                                     [#{name => Topic,
                                        partitions =>
                                        [#{error_code => offset_out_of_range,
                                           high_watermark_offset => -1,
                                           partition => Partition}]}]}}
                           end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = undefined,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
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
  meck:expect(kafe, offset, fun([{Topic, [{Partition, -1, 1}]}]) ->
                                {ok, [#{name => Topic,
                                        partitions => [#{error_code => none,
                                                         id => Partition,
                                                         offsets => [102]}]}]}
                            end),
  meck:expect(kafe, fetch, fun(_, _, _) ->
                               {error, test_error}
                           end),

  ?assertMatch(#state{fetch_interval = 100,
                      topic = <<"topic">>,
                      partition = 10,
                      offset = 99,
                      commit = undefined,
                      min_bytes = 1,
                      max_bytes = 10000,
                      max_wait_time = 10,
                      callback = _},
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

perform_fetch_without_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(102,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>},
                              #{offset => 102, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(102,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
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

perform_fetch_with_invalid_processing_commit_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
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

perform_fetch_with_commit_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, {error, test_error}),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> ok end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
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

perform_fetch_with_callback_exception_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, <<"bad match">>) -> ok end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
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

perform_fetch_with_callback_error_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> {error, test_error} end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
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

perform_fetch_with_invalid_callback_response_test() ->
  meck:new(kafe_consumer),
  meck:expect(kafe_consumer, commit, 4, ok),
  meck:new(kafe_metrics, [passthrough]),
  meck:expect(kafe_metrics, consumer_partition_duration, 4, ok),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
                              #{offset => 101, key => <<"key">>, value => <<"value">>}],
                             <<"topic">>,
                             1,
                             after_processing,
                             srv,
                             fun(_, _, _, _, _, _) -> invalid_test_response end,
                             100,
                             0)),
  ?assertEqual(100,
               perform_fetch([#{offset => 100, key => <<"key">>, value => <<"value">>},
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
               commit(offset, topic, partition, group, before_processing, before_processing)),
  ?assertEqual(commit_result,
               commit(offset, topic, partition, group, after_processing, after_processing)),
  ?assertEqual(ok,
               commit(offset, topic, partition, group, after_processing, before_processing)),
  ?assertEqual(ok,
               commit(offset, topic, partition, group, before_processing, after_processing)),
  ?assertEqual(ok,
               commit(offset, topic, partition, group, undefined, before_processing)),
  ?assertEqual(ok,
               commit(offset, topic, partition, group, undefined, after_processing)),
  ?assertEqual({error, invalid_processing},
               commit(offset, topic, partition, group, invalid, after_processing)),
  ?assertEqual({error, invalid_processing},
               commit(offset, topic, partition, group, invalid, before_processing)),
  ?assertEqual({error, invalid_processing},
               commit(offset, topic, partition, group, before_processing, invalid)),
  ?assertEqual({error, invalid_processing},
               commit(offset, topic, partition, group, after_processing, invalid)),
  ?assertEqual({error, invalid_processing},
               commit(offset, topic, partition, group, undefined, invalid)),
  ?assertEqual({error, invalid_processing},
               commit(offset, topic, partition, group, invalid, invalid)),

  meck:unload(kafe_consumer).
-endif.

