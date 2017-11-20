-module(kafe_cluster_SUITE).
-compile([{parse_transform, lager_transform}]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
         t_broker_going_down_while_consuming/1
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

wait_until(F, Code, Timeout) ->
  try
    F()
  catch
    _:_ when Timeout > 0 ->
      lager:debug("Evaluation of '~s' failed, waiting before retrying...", [Code]),
      timer:sleep(1000),
      wait_until(F, Code, Timeout-1000)
  end.

-define(RETRY(Expr), wait_until(fun () -> Expr end, ??Expr, 10000)).

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
  ?RETRY(true = ets:lookup_element(Table, fetching, 2)),
  kafe_test_cluster:down(["kafka1"]),
  ?RETRY(false = ets:lookup_element(Table, fetching, 2)),
  kafe_test_cluster:up(["kafka1"]),
  ?RETRY(true = ets:lookup_element(Table, fetching, 2)),
  ok.
