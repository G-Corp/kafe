-module(kafe_protocol_tests).
-include_lib("eunit/include/eunit.hrl").

kafe_protocol_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_protocol_request()),
    ?_test(t_protocol_encode_string()),
    ?_test(t_protocol_encode_bytes()),
    ?_test(t_protocol_encode_array()),
    ?_test(t_protocol_encode_decode_varint()),
    ?_test(t_protocol_encode_decode_batch())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_protocol_request() ->
  ?assertEqual(#{packet => <<0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116>>,
                 state => #{
                   api_version => 0,
                   api_key => 1,
                   correlation_id => 1,
                   client_id => <<"test">>}},
                 kafe_protocol:request(<<>>, #{api_version => 0,
                                               api_key => 1,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),
  ?assertEqual(#{packet => <<0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116>>,
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

t_protocol_encode_decode_varint() ->
  ?assertEqual({922337203685477580, <<>>},
               kafe_protocol:decode_varint(
                 kafe_protocol:encode_varint(922337203685477580))).

t_protocol_encode_decode_batch() ->
  Message1 = <<
               0:8/signed                                 % Attributes
               , (kafe_protocol:encode_varint(0))/binary  % TimestampDelta
               , (kafe_protocol:encode_varint(0))/binary  % OffsetDelta
               , (kafe_protocol:encode_varint(4))/binary  % KeyLen
               , "key1"                                   % Key
               , (kafe_protocol:encode_varint(6))/binary  % ValueLen
               , "value1"                                 % Value
               , 0:32/signed                              % Headers count
             >>,
  Message2 = <<
               0:8/signed                               % Attributes
               , (kafe_protocol:encode_varint(1))/binary  % TimestampDelta
               , (kafe_protocol:encode_varint(1))/binary  % OffsetDelta
               , (kafe_protocol:encode_varint(4))/binary  % KeyLen
               , "key2"                                   % Key
               , (kafe_protocol:encode_varint(6))/binary  % ValueLen
               , "value2"                                 % Value
               , 2:32/signed                              % Headers count
                 , (kafe_protocol:encode_varint(7))/binary  % HeaderKeyLen
                 , "header1"                                % HeaderKey
                 , (kafe_protocol:encode_varint(8))/binary  % HeaderValueLen
                 , "headerv1"                               % HeaderValue
                 , (kafe_protocol:encode_varint(7))/binary  % HeaderKeyLen
                 , "header2"                                % HeaderKey
                 , (kafe_protocol:encode_varint(8))/binary  % HeaderValueLen
                 , "headerv2"                               % HeaderValue
             >>,
  Body0 = <<
            0:16/signed            % Attributes
            , 1:32/signed          % LastOffsetDelta
            , 1234567890:64/signed % FirstTimestamp
            , 1234567891:64/signed % MaxTimestamp
            , -1:64/signed         % ProducerID
            , -1:16/signed         % ProducerEpoch
            , -1:32/signed         % FirstSequence
            , 2:32/signed          % Records count
              , (kafe_protocol:encode_varint(size(Message1)))/binary, Message1/binary
              , (kafe_protocol:encode_varint(size(Message2)))/binary, Message2/binary
          >>,
  CRC = erlang:crc32(Body0),
  Body1 = <<
            -1:32/signed    % PartitionLeaderEpoch
            , 2:8/signed    % Magic
            , CRC:32/signed % CRC
            , Body0/binary
          >>,
  Batch = <<0:64/signed               % FirstOffset
            , (size(Body1)):32/signed % Length
            , Body1/binary
          >>,
  ?assertMatch(
     Batch,
     kafe_protocol:encode_record_batch(
       [{<<"key1">>, <<"value1">>, 1234567890},
        {<<"key2">>, <<"value2">>, 1234567891, [{<<"header1">>, <<"headerv1">>},
                                                {<<"header2">>, <<"headerv2">>}]}],
       #{}
      )).
