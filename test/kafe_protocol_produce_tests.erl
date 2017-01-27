-module(kafe_protocol_produce_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").
-include("../include/kafe.hrl").

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
  ?assertEqual(
     #{api_version => 0,
       packet => <<0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193, 236,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 0,
                                     correlation_id => 0,
                                     client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193, 236,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 1,
                                     correlation_id => 0,
                                     client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193, 236,
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>}]}]}],
                                   #{timestamp => 1460103641930371},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 1,
                                     correlation_id => 0,
                                     client_id => <<"test">>})),

  ?assertEqual(
     #{api_version => 1,
       packet => <<0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 0, 0, 19, 136, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 86, 91, 193,
                   236, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 0, 0, 0,
                   0, 0, 0, 0, 0, 0, 0, 0, 24, 162, 47, 233, 121, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 104, 111, 108, 97, 32,
                   109, 117, 110, 100, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 227, 127, 239, 189, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 16, 98, 111, 110, 106, 111, 117, 114, 32, 108, 101, 32, 109, 111, 110, 100, 101>>,
       state => #{api_key => ?PRODUCE_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_produce:request([{<<"topic">>, [{0, [{<<>>, <<"hello world">>},
                                                        {<<>>, <<"hola mundo">>},
                                                        {<<>>, <<"bonjour le monde">>}]}]}],
                                   #{timestamp => 1460103641930371},
                                   #{api_key => ?PRODUCE_REQUEST,
                                     api_version => 1,
                                     correlation_id => 0,
                                     client_id => <<"test">>})).

t_response() ->
  ?assertEqual({ok, [#{name => <<"topic">>,
                       partitions => [#{error_code => none, offset => 5, partition => 0}]}]},
               kafe_protocol_produce:response(
                 <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                   0, 0, 5>>,
                 x, % TODO delete
                 #{api_version => 0})).

