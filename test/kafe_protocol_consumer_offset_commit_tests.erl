-module(kafe_protocol_consumer_offset_commit_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

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
     #{packet => <<0, 8, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115, 117, 109, 101,
                   114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0,
                   0, 0, 0, 0, 0, 0, 0, 3, 232, 255, 255, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 3, 233, 0, 8, 109, 101,
                   116, 97, 100, 97, 116, 97, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 3, 234, 255, 255>>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v0(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [{0, 1000}, {1, 1001, <<"metadata">>}, {2, 1002}]}],
       #{api_version => 0,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => <<0, 8, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115, 117, 109, 101,
                   114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 10, 67, 111, 110, 115, 117, 109, 101, 114,
                   73, 100, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
                   232, 0, 0, 0, 0, 7, 91, 205, 21, 255, 255, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 3, 233, 0, 0, 0, 0, 7,
                   91, 205, 21, 0, 8, 109, 101, 116, 97, 100, 97, 116, 97, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 3, 234,
                   0, 0, 0, 0, 7, 91, 205, 21, 255, 255>>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v1(
       <<"ConsumerGroup">>,
       1,
       <<"ConsumerId">>,
       [{<<"topic">>, [{0, 1000, 123456789}, {1, 1001, 123456789, <<"metadata">>}, {2, 1002, 123456789}]}],
       #{api_version => 1,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => <<0, 8, 0, 2, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115, 117, 109, 101,
                   114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 10, 67, 111, 110, 115, 117, 109, 101, 114,
                   73, 100, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0,
                   0, 0, 0, 0, 0, 0, 0, 3, 232, 255, 255, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 3, 233, 0, 8, 109, 101,
                   116, 97, 100, 97, 116, 97, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 3, 234, 255, 255>>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v2(
       <<"ConsumerGroup">>,
       1,
       <<"ConsumerId">>,
       3,
       [{<<"topic">>, [{0, 1000}, {1, 1001, <<"metadata">>}, {2, 1002}]}],
       #{api_version => 2,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>,
       #{api_version => 0})),
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>,
       #{api_version => 1})),
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>,
       #{api_version => 2})).

