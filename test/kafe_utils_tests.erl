-module(kafe_utils_tests).

-include_lib("eunit/include/eunit.hrl").

kafe_utils_test_() ->
  {setup, fun setup/0, fun teardown/1, 
   [
    ?_test(t_broker_id()),
    ?_test(t_broker_name())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_broker_id() ->
  ?assertEqual(kafe_utils:broker_id(<<"localhost">>, 8080),
               'localhost:8080').

t_broker_name() ->
  ?assertEqual(kafe_utils:broker_name(<<"localhost">>, 8080),
               "localhost:8080").

