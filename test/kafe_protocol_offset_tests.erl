-module(kafe_protocol_offset_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

kafe_protocol_offset_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request()),
    ?_test(t_response())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request() ->
  ?assertEqual(kafe_protocol_offset:request(-1, [{<<"topic">>, [0,1,2]}], ?REQ_STATE(0)),
               #{packet => <<0,0,0,33,0,2,0,1,0,0,0,0,0,4,116,101,115,116,255,255,255,255,
                             0,0,0,1,0,5,116,111,112,105,99,0,0,0,0>>,
                 state => ?REQ_STATE(1)}).

t_response() ->
  ?assertEqual(kafe_protocol_offset:response(
                 <<0,0,0,1,0,5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,0,0,2,0,0,
                                   0,0,0,0,0,5,0,0,0,0,0,0,0,0>>),
               {ok,[#{name => <<"topic">>,
                      partitions => [#{error_code => no_error,id => 0,offsets => [5,0]}]}]}).
