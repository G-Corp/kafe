-module(kafe_internal_api_SUITE).
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
         t_topics/1
         , t_number_of_brokers/1
         , t_partitions/1
         , t_api_version/1
        ]).

suite() ->
   [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  kafe_test_cluster:up(),
  {ok, _} = application:ensure_all_started(kafe),
  Config.

end_per_suite(_Config) ->
  application:stop(kafe),
  application:stop(poolgirl),
  ok.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_Case, Config) ->
  Config.

all() ->
  [F||{F, _A} <- module_info(exports),
      case atom_to_list(F) of
        "t_" ++ _ -> true;
        _ -> false
      end].

t_topics(_Config) ->
  ?RETRY(#{<<"testone">> := #{0 := _},
           <<"testthree">> := #{0 := _,
                                1 := _,
                                2 := _},
           <<"testtwo">> := #{0 := _,
                              1 := _}} = kafe:topics()).

t_number_of_brokers(_Config) ->
  ?RETRY(3 = kafe:number_of_brokers()).

t_partitions(_Config) ->
  ?RETRY([0] = kafe:partitions(<<"testone">>)),
  ?RETRY([0, 1] = kafe:partitions(<<"testtwo">>)),
  ?RETRY([0, 1, 2] = kafe:partitions(<<"testthree">>)).

t_api_version(_Config) ->
  ?RETRY(auto = kafe:api_version()).

