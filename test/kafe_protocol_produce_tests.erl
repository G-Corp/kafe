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
  ?assertEqual(#{packet => <<0,0,0,89,0,0,0,0,0,0,0,0,0,4,116,101,115,116,255,255,0,0,19,136,0,0,0,1,0,
                             5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,46,0,0,0,0,0,0,0,0,0,0,0,34,65,
                             138,154,111,0,0,0,0,0,9,117,110,100,101,102,105,110,101,100,0,0,0,11,104,
                             101,108,108,111,32,119,111,114,108,100>>,
                 state => ?REQ_STATE2(1, 0)},
               kafe_protocol_produce:request(<<"topic">>, <<"hello world">>, #{}, ?REQ_STATE2(0, 0))),
  ?assertEqual(#{packet => <<0,0,0,89,0,0,0,1,0,0,0,0,0,4,116,101,115,116,255,255,0,0,19,136,0,0,0,1,0,
                             5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,46,0,0,0,0,0,0,0,0,0,0,0,34,125,
                             234,121,103,1,0,0,0,0,9,117,110,100,101,102,105,110,101,100,0,0,0,11,104,
                             101,108,108,111,32,119,111,114,108,100>>,
                 state => ?REQ_STATE2(1, 1)},
               kafe_protocol_produce:request(<<"topic">>, <<"hello world">>, #{}, ?REQ_STATE2(0, 1))),
  ?assertEqual(#{packet => <<0,0,0,97,0,0,0,2,0,0,0,0,0,4,116,101,115,116,255,255,0,0,19,136,0,0,0,1,0,
                             5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,54,0,0,0,0,0,0,0,0,0,0,0,42,216,
                             52,168,36,2,0,0,5,47,244,222,233,154,131,0,0,0,9,117,110,100,101,102,105,
                             110,101,100,0,0,0,11,104,101,108,108,111,32,119,111,114,108,100>>,
                 state => ?REQ_STATE2(1, 2)},
               kafe_protocol_produce:request(<<"topic">>, <<"hello world">>, #{timestamp => 1460103641930371}, ?REQ_STATE2(0, 2))).

t_response() ->
  ?assertEqual({ok,[#{name => <<"topic">>,
                      partitions => [#{error_code => none, offset => 5, partition => 0}]}]},
               kafe_protocol_produce:response(
                 <<0,0,0,1,0,5,116,111,112,105,99,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,
                   0,0,5>>, 0)).

