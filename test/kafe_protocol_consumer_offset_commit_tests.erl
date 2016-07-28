-module(kafe_protocol_consumer_offset_commit_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

kafe_protocol_consumer_offset_commit_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request())
    , ?_test(t_response())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request() ->
  ?assertEqual(
     #{api_version => 0,
       packet =>
       <<0, 0, 0, 44, 0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111,
         110, 115, 117, 109, 101, 114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 5,
         116, 111, 112, 105, 99, 0, 0, 0, 0>>,
       state => ?REQ_STATE2(1, 0)},
     kafe_protocol_consumer_offset_commit:request_v0(
       <<"ConsumerGroup">>, [{<<"topic">>, [0, 1, 2]}], ?REQ_STATE2(0, 0))),
  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 0, 0, 60, 0, 8, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115, 117, 109,
                   101, 114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 10, 67, 111, 110, 115, 117, 109, 101, 114, 73,
                   100, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 0>>,
       state => ?REQ_STATE2(1, 1)},
     kafe_protocol_consumer_offset_commit:request_v1(
       <<"ConsumerGroup">>, 1, <<"ConsumerId">>, [{<<"topic">>, [0, 1, 2]}], ?REQ_STATE2(0, 1))),
  ?assertEqual(
     #{api_version => 2,
       packet => <<0, 0, 0, 68, 0, 8, 0, 2, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115, 117, 109,
                   101, 114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 10, 67, 111, 110, 115, 117, 109, 101, 114, 73,
                   100, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 0>>,
       state => ?REQ_STATE2(1, 2)},
     kafe_protocol_consumer_offset_commit:request_v2(
       <<"ConsumerGroup">>, 1, <<"ConsumerId">>, 3, [{<<"topic">>, [0, 1, 2]}], ?REQ_STATE2(0, 2))).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>, 0)).

