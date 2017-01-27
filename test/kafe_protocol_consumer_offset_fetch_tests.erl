-module(kafe_protocol_consumer_offset_fetch_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_consumer_offset_fetch_test_() ->
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
       packet => <<0, 9, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115,
                   117, 109, 101, 114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105,
                   99, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2>>,
       state => #{
         api_version => 0,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_consumer_offset_fetch:request(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [0, 1, 2]}],
       #{api_version => 0,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 9, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 13, 67, 111, 110, 115,
                   117, 109, 101, 114, 71, 114, 111, 117, 112, 0, 0, 0, 1, 0, 5, 116, 111, 112, 105,
                   99, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2>>,
       state => #{
         api_version => 1,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_consumer_offset_fetch:request(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [0, 1, 2]}],
       #{api_version => 1,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})).

t_response() ->
  Data = <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 255, 255, 255,
           255, 255, 255, 255, 255, 0, 0, 0, 3, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255,
           255, 255, 0, 0, 0, 3, 0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0,
           0, 3>>,
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions_offset => [#{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 2},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 1},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 0}]}]},
     kafe_protocol_consumer_offset_fetch:response(
       Data,
       x, % TODO delete
       #{api_version => 0})),
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions_offset => [#{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 2},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 1},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 0}]}]},
     kafe_protocol_consumer_offset_fetch:response(
       Data,
       x, % TODO delete
       #{api_version => 1})).

