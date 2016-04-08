-module(kafe_protocol_metadata_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

kafe_protocol_metadata_test_() ->
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
  ?assertEqual(kafe_protocol_metadata:request([<<"topic">>], ?REQ_STATE(0)),
               #{packet => <<0,0,0,25,0,3,0,1,0,0,0,0,0,4,116,101,115,116,0,0,0,1,0,5,116,111,
                             112,105,99>>,
                 state => ?REQ_STATE(1)}).

t_response() ->
  ?assertEqual(kafe_protocol_metadata:response(
                 <<0,0,0,3,0,0,0,0,0,12,49,55,50,46,49,54,46,52,50,46,49,51,0,0,35,
                   132,0,0,0,1,0,12,49,55,50,46,49,54,46,52,50,46,49,51,0,0,35,133,
                   0,0,0,2,0,12,49,55,50,46,49,54,46,52,50,46,49,51,0,0,35,134,0,0,
                   0,1,0,0,0,5,116,111,112,105,99,0,0,0,3,0,0,0,0,0,2,0,0,0,2,0,0,
                   0,3,0,0,0,2,0,0,0,0,0,0,0,1,0,0,0,3,0,0,0,2,0,0,0,0,0,0,0,1,0,0,
                   0,0,0,1,0,0,0,1,0,0,0,3,0,0,0,1,0,0,0,2,0,0,0,0,0,0,0,3,0,0,0,1,
                   0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,1,0,0,
                   0,2,0,0,0,3,0,0,0,0,0,0,0,1,0,0,0,2>>, 2),
                 {ok,#{brokers => [#{host => <<"172.16.42.13">>,id => 2,port => 9094},
                                   #{host => <<"172.16.42.13">>,id => 1,port => 9093},
                                   #{host => <<"172.16.42.13">>,id => 0,port => 9092}],
                       topics => [#{error_code => none,
                                    name => <<"topic">>,
                                    partitions => [#{error_code => none,
                                                     id => 0,
                                                     isr => [2,1,0],
                                                     leader => 0,
                                                     replicas => [2,1,0]},
                                                   #{error_code => none,
                                                     id => 1,
                                                     isr => [0,2,1],
                                                     leader => 1,
                                                     replicas => [0,2,1]},
                                                   #{error_code => none,
                                                     id => 2,
                                                     isr => [1,0,2],
                                                     leader => 2,
                                                     replicas => [1,0,2]}]}]}}).

