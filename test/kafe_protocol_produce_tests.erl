-module(kafe_protocol_produce_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

kafe_protocol_produce_test_() ->
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
  ?assertEqual(kafe_protocol_produce:request(<<"topic">>, <<"hello world">>, #{}, ?REQ_STATE(0)),
               #{packet => <<0,0,0,80,0,0,0,1,0,0,0,0,0,4,116,101,115,116,0,1,0,0,19,136,0,0,0,
                             1,0,5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,37,0,0,0,0,0,0,0,0,
                             0,0,0,25,236,118,116,226,1,0,255,255,255,255,0,0,0,11,104,101,108,
                             108,111,32,119,111,114,108,100>>,
                 state => ?REQ_STATE(1)}).

t_response() ->
  ?assertEqual(kafe_protocol_produce:response(
                 <<0,0,0,1,0,5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,
                   0,0,5>>),
               {ok,[#{name => <<"topic">>,
                      partitions => [#{error_code => none, offset => 5, partition => 0}]}]}).

