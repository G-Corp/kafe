-module(kafe_protocol_tests).
-include_lib("eunit/include/eunit.hrl").

kafe_protocol_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_protocol_request()),
    ?_test(t_protocol_encode_string()),
    ?_test(t_protocol_encode_bytes()),
    ?_test(t_protocol_encode_array())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_protocol_request() ->
  ?assertEqual(#{api_version => 0,
                 packet => <<0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116>>,
                 state => #{
                   api_version => 0,
                   api_key => 1,
                   correlation_id => 1,
                   client_id => <<"test">>}},
                 kafe_protocol:request(<<>>, #{api_version => 0,
                                               api_key => 1,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),
  ?assertEqual(#{api_version => 1,
                 packet => <<0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116>>,
                 state => #{
                   api_version => 1,
                   api_key => 1,
                   correlation_id => 1,
                   client_id => <<"test">>}},
                 kafe_protocol:request(<<>>, #{api_version => 1,
                                               api_key => 1,
                                               correlation_id => 0,
                                               client_id => <<"test">>})).

t_protocol_encode_string() ->
  ?assertEqual(kafe_protocol:encode_string(undefined),
               <<-1:16/signed>>),
  ?assertEqual(kafe_protocol:encode_string(<<"hello">>),
               <<0, 5, 104, 101, 108, 108, 111>>).

t_protocol_encode_bytes() ->
  ?assertEqual(kafe_protocol:encode_bytes(undefined),
               <<-1:32/signed>>),
  ?assertEqual(kafe_protocol:encode_bytes(<<"hello">>),
               <<0, 0, 0, 5, 104, 101, 108, 108, 111>>).

t_protocol_encode_array() ->
  ?assertEqual(kafe_protocol:encode_array([<<"a">>, <<"b">>, <<"c">>, <<"d">>]),
               <<0, 0, 0, 4, 97, 98, 99, 100>>).

