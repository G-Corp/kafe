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
         t_brokers_going_up_and_down/1
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

wait_until(F, Timeout) ->
  try
    F()
  catch
    _:_ when Timeout > 0 ->
      timer:sleep(1000),
      wait_until(F, Timeout-1000)
  end.

-define(RETRY(Expr),
  begin
    wait_until(fun () -> Expr end, 10000)
  end).

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
