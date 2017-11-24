-module(kafe_cluster_SUITE).
-compile([{parse_transform, lager_transform}]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("kafe_ct_common.hrl").

-export([
         init_per_suite/1
         , end_per_suite/1
         , init_per_testcase/2
         , end_per_testcase/2
         , all/0
         , suite/0
        ]).

-export([
         t_bootstrap_fail/1,
         t_single_broker/1,
         t_bootstrap_to_cluster/1,
         t_brokers_going_up_and_down/1,
         t_broker_going_down_while_consuming/1,
         t_broker_not_yet_available_when_starting_consumer/1
        ]).

suite() ->
  [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(lager),
  Config.

end_per_suite(_Config) ->
  ok.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_Case, Config) ->
  application:stop(kafe),
  application:stop(poolgirl),
  ok = application:unload(kafe),
  Config.

all() ->
  [F||{F, _A} <- module_info(exports),
      case atom_to_list(F) of
        "t_" ++ _ -> true;
        _ -> false
      end].

start_kafe(Brokers) ->
  ok = application:load(kafe),
  ok = application:set_env(kafe, brokers, Brokers),
  {ok, _} = application:ensure_all_started(kafe).

t_bootstrap_fail(_Config) ->
  kafe_test_cluster:up(["kafka2", "kafka3"]),
  kafe_test_cluster:down(["kafka1"]),
  start_kafe([{"localhost", 9191}]),
  ?RETRY([] = kafe:brokers()).

t_single_broker(_Config) ->
  kafe_test_cluster:up(["kafka1"]),
  kafe_test_cluster:down(["kafka2", "kafka3"]),
  start_kafe([{"localhost", 9191}]),
  ?RETRY(["kafka1:9191"] = kafe:brokers()).

t_bootstrap_to_cluster(_Config) ->
  kafe_test_cluster:up(["kafka1", "kafka2", "kafka3"]),
  start_kafe([{"localhost", 9191}]),
  ?RETRY(["kafka1:9191", "kafka2:9192", "kafka3:9193"] = kafe:brokers()).

t_brokers_going_up_and_down(_Config) ->
  kafe_test_cluster:up(["kafka2", "kafka3"]),
  kafe_test_cluster:down(["kafka1"]),
  start_kafe([{"localhost", 9191}]),
  ?RETRY([] = kafe:brokers()),
  kafe_test_cluster:up(["kafka1"]),
  ?RETRY(["kafka1:9191", "kafka2:9192", "kafka3:9193"] = kafe:brokers()),
  kafe_test_cluster:down(["kafka1"]),
  ?RETRY(["kafka2:9192", "kafka3:9193"] = kafe:brokers()),
  kafe_test_cluster:down(["kafka3"]),
  ?RETRY(["kafka2:9192"] = kafe:brokers()),
  kafe_test_cluster:up(["kafka1"]),
  ?RETRY(["kafka1:9191", "kafka2:9192"] = kafe:brokers()).

consume(GroupID, Topic, Partition, Offset, Key, Value) ->
  lager:info("[~p] ~p/~p (~p): ~p ~p", [GroupID, Topic, Partition, Offset, Key, Value]),
  ok.

make_group_name() ->
  {Meg, Sec, Mic} = erlang:timestamp(),
  lists:flatten(io_lib:format("~6..0B_~6..0B_~6..0B", [Meg, Sec, Mic])).

t_broker_going_down_while_consuming(_Config) ->
  kafe_test_cluster:up(["kafka1"]),
  kafe_test_cluster:down(["kafka2", "kafka3"]),
  start_kafe([{"localhost", 9191}]),
  ?RETRY(["kafka1:9191"] = kafe:brokers()),
  Table = ets:new(consumer_state, [public]),
  ets:insert(Table, {fetching, false}),
  {ok, _Pid} = kafe:start_consumer(make_group_name(),
                      fun consume/6,
                      #{
                       topics => [{<<"testone">>, [0]}],
                       fetch_interval => 1000,
                       on_start_fetching => fun (GroupId) ->
                                              lager:info("start fetching ~p", [GroupId]),
                                              ets:insert(Table, {fetching, true}),
                                              ok
                                            end,
                       on_stop_fetching => fun (GroupId) ->
                                              lager:info("stop fetching ~p", [GroupId]),
                                              ets:insert(Table, {fetching, false}),
                                              ok
                                            end
                      }),
  ?RETRY([{fetching, true}] = ets:lookup(Table, fetching)),
  kafe_test_cluster:down(["kafka1"]),
  ?RETRY([{fetching, false}] = ets:lookup(Table, fetching)),
  kafe_test_cluster:up(["kafka1"]),
  ?RETRY([{fetching, true}] = ets:lookup(Table, fetching)),
  ok.

t_broker_not_yet_available_when_starting_consumer(_Config) ->
  kafe_test_cluster:up(["kafka1"]),
  kafe_test_cluster:down(["kafka2", "kafka3"]),
  Table = ets:new(consumer_state, [public]),
  ets:insert(Table, {fetching, false}),
  start_kafe([{"localhost", 9191}]),
  %?RETRY(["kafka1:9191"] = kafe:brokers()),
  ExpectedValue = list_to_binary(make_group_name()),
  ?RETRY({ok, _Pid} = kafe:start_consumer(make_group_name(),
                      fun (_GroupID, _Topic, _Partition, _Offset, _Key, Value)->
                          ets:insert(Table, {fetched, Value}),
                          ok
                      end,
                      #{
                       topics => [<<"testone">>],
                       fetch_interval => 100,
                       on_start_fetching => fun (GroupId) ->
                                              lager:info("start fetching ~p", [GroupId]),
                                              ets:insert(Table, {fetching, true}),
                                              ok
                                            end
                      })),
  ?RETRY({ok, _} = kafe:produce([{<<"testone">>, [{ExpectedValue, 0}]}])),
  ?RETRY([{fetching, true}] = ets:lookup(Table, fetching)),
  ?RETRY([{fetched, ExpectedValue}] = ets:lookup(Table, fetched)),
  ok.
