-module(kafe_protocol_metadata_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").
-include("../include/kafe.hrl").

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
  ?assertEqual(
     #{api_version => 0,
       packet => <<0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0>>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 0,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{api_key => ?METADATA_REQUEST,
                                          api_version => 0,
                                          correlation_id => 0,
                                          client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 3, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255>>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 1,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{api_version => 1,
                                          api_key => ?METADATA_REQUEST,
                                          correlation_id => 0,
                                          client_id => <<"test">>})),
  ?assertEqual(
     #{api_version => 2,
       packet => <<0, 3, 0, 2, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255>>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 2,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{api_version => 2,
                                          api_key => ?METADATA_REQUEST,
                                          correlation_id => 0,
                                          client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 0,
       packet => <<0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 2, 0, 6, 116, 111, 112, 105, 99, 49, 0, 6, 116, 111, 112, 105, 99, 50>>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{api_version => 0,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 3, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 2, 0, 6, 116, 111, 112, 105, 99, 49, 0, 6, 116, 111, 112, 105, 99, 50>>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{api_version => 1,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 2,
       packet => <<0, 3, 0, 2, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 2, 0, 6, 116, 111, 112, 105, 99, 49, 0, 6, 116, 111, 112, 105, 99, 50>>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{api_version => 2,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})).




t_response() ->
  ?assertEqual(
     {ok, #{brokers => [#{host => <<"172.17.0.1">>,
                          id => 3,
                          port => 9094},
                        #{host => <<"172.17.0.1">>,
                          id => 1,
                          port => 9092},
                        #{host => <<"172.17.0.1">>,
                          id => 2,
                          port => 9093}],
            topics => [#{error_code => none,
                         name => <<"topic">>,
                         partitions => [#{error_code => none,
                                          id => 0,
                                          isr => [3, 2, 1],
                                          leader => 1,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 1,
                                          isr => [1, 3, 2],
                                          leader => 2,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 2,
                                          isr => [2, 1, 3],
                                          leader => 3,
                                          replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<0, 0, 0, 3, 0, 0, 0, 2, 0, 10, 49, 55, 50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 133, 0, 0, 0, 1, 0, 10,
         49, 55, 50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 132, 0, 0, 0, 3, 0, 10, 49, 55, 50, 46, 49, 55, 46,
         48, 46, 49, 0, 0, 35, 134, 0, 0, 0, 1, 0, 0, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0,
         0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0,
         0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
         1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0,
         2, 0, 0, 0, 3>>,
       x, % TODO remove
       #{api_version => 0})),

  ?assertEqual(
     {ok, #{brokers => [#{host => <<"172.17.0.1">>,
                          id => 3,
                          port => 9094,
                          rack => <<>>},
                        #{host => <<"172.17.0.1">>,
                          id => 1,
                          port => 9092,
                          rack => <<>>},
                        #{host => <<"172.17.0.1">>,
                          id => 2,
                          port => 9093,
                          rack => <<>>}],
            controller_id => 1,
            topics => [#{error_code => none,
                         is_internal => false,
                         name => <<"topic">>,
                         partitions => [#{error_code => none,
                                          id => 0,
                                          isr => [3, 2, 1],
                                          leader => 1,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 1,
                                          isr => [1, 3, 2],
                                          leader => 2,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 2,
                                          isr => [2, 1, 3],
                                          leader => 3,
                                          replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(

       <<0, 0, 0, 3, 0, 0, 0, 2, 0, 10, 49, 55, 50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 133, 255, 255, 0, 0, 0,
         1, 0, 10, 49, 55, 50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 132, 255, 255, 0, 0, 0, 3, 0, 10, 49, 55,
         50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 134, 255, 255, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 5, 116, 111,
         112, 105, 99, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0,
         0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0,
         0, 3, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0,
         0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3>>,
       x, % TODO delete
       #{api_version => 1})),

  ?assertEqual(
     {ok,
      #{brokers => [#{host => <<"172.17.0.1">>,
                      id => 3,
                      port => 9094,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 1,
                      port => 9092,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 2,
                      port => 9093,
                      rack => <<>>}],
        cluster_id => <<"T7X1Bh3RSVmQTqhbXB81gg">>,
        controller_id => 1,
        topics => [#{error_code => none,
                     is_internal => false,
                     name => <<"topic">>,
                     partitions => [#{error_code => none,
                                      id => 0,
                                      isr => [3, 2, 1],
                                      leader => 1,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 1,
                                      isr => [1, 3, 2],
                                      leader => 2,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 2,
                                      isr => [2, 1, 3],
                                      leader => 3,
                                      replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<0, 0, 0, 3, 0, 0, 0, 2, 0, 10, 49, 55, 50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 133, 255, 255, 0, 0, 0,
         1, 0, 10, 49, 55, 50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 132, 255, 255, 0, 0, 0, 3, 0, 10, 49, 55,
         50, 46, 49, 55, 46, 48, 46, 49, 0, 0, 35, 134, 255, 255, 0, 22, 84, 55, 88, 49, 66, 104, 51, 82, 83,
         86, 109, 81, 84, 113, 104, 98, 88, 66, 56, 49, 103, 103, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 5, 116, 111,
         112, 105, 99, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0,
         0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0,
         0, 3, 0, 0, 0, 3, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0,
         0, 2, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3>>,
       x, % TODO remove
       #{api_version => 2})).

