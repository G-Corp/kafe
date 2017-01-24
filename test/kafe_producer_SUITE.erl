-module(kafe_producer_SUITE).

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
         , t_produce/1
         , t_produce_no_ack/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
   [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  application:ensure_all_started(kafe),
  Config.

end_per_suite(_Config) ->
  application:stop(kafe),
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
  ?assertMatch(
     #{<<"testone">> := #{0 := _},
       <<"testthree">> := #{0 := _,
                            1 := _,
                            2 := _},
       <<"testtwo">> := #{0 := _,
                          1 := _}},
     kafe:topics()).

t_produce(_Config) ->
  1 = 1.

t_produce_no_ack(_Config) ->
  1 = 1.
