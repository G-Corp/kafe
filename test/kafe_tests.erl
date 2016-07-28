-module(kafe_tests).

-include_lib("eunit/include/eunit.hrl").

kafe_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_protocol())
   ]
  }.

setup() ->
  %[application:ensure_all_started(App) || App <- [kafe]],
  ok.

teardown(_) ->
  %[application:stop(X) || X <- [kafe, lager, goldrush]],
  ok.

t_protocol() ->
  ?assert(true).
