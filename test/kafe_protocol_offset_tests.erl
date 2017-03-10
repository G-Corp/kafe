-module(kafe_protocol_offset_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_offset_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request()),
    ?_test(t_response())
   ]
  }.

setup() ->
  meck:new(kafe, [passthrough]),
  meck:expect(kafe, partitions, 1, [0, 1, 2]),
  ok.

teardown(_) ->
  meck:unload(kafe),
  ok.

t_request() ->
  ?assertEqual(
     #{packet => <<0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 255, 255, 0, 0,
                   0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 255, 255, 0, 0, 0, 2, 255, 255, 255, 255,
                   255, 255, 255, 255, 0, 0, 255, 255>>,
       state => #{api_key => ?OFFSET_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_offset:request(-1,
                                  [<<"topic">>],
                                  #{api_key => ?OFFSET_REQUEST,
                                    api_version => 0,
                                    correlation_id => 0,
                                    client_id => <<"test">>})),
  ?assertEqual(
     #{packet => <<0, 2, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 255, 255, 255, 255, 0, 0, 0, 1, 0, 5, 116, 111,
                   112, 105, 99, 0, 0, 0, 3, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 255, 255,
                   255, 255, 255, 255, 255, 255, 0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255>>,
       state => #{api_key => ?OFFSET_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_offset:request(-1,
                                  [<<"topic">>],
                                  #{api_key => ?OFFSET_REQUEST,
                                    api_version => 1,
                                    correlation_id => 0,
                                    client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              id => 0,
                              offsets => [4, 0]}]}]},
     kafe_protocol_offset:response(
       <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0,
         0, 0, 0, 0, 0, 0, 0>>,
       #{api_version => 0})),

  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              id => 0,
                              offset => 4,
                              timestamp => -1}]}]},
     kafe_protocol_offset:response(
       <<0, 0, 0, 1, 0, 5, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 4>>,
       #{api_version => 1})).

