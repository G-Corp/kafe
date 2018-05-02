% @hidden
-module(kafe_protocol).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/4
         , run/5
         , request/2
         , response/2
         , encode_string/1
         , encode_bytes/1
         , encode_array/1
         , encode_boolean/1
         , encode_varint/1
         , decode_varint/1
         , encode_record_batch/2
        ]).

-define(VARINT_MAX_BITS, 63).

run(ApiKey, MaxVersion, RequestFun, ResponseFun) when is_integer(ApiKey),
                                                      is_integer(MaxVersion) ->
  run(ApiKey, MaxVersion, RequestFun, ResponseFun, #{}).

run(ApiKey, MaxVersion, RequestFun, ResponseFun, State) when is_integer(ApiKey),
                                                             is_integer(MaxVersion),
                                                             is_function(RequestFun),
                                                             is_map(State)->
  run(ApiKey, MaxVersion, {RequestFun, []}, ResponseFun, State);
run(ApiKey, MaxVersion, RequestFun, ResponseFun, State) when is_integer(ApiKey),
                                                             is_integer(MaxVersion),
                                                             (is_function(ResponseFun) orelse ResponseFun == undefined),
                                                             is_map(State)->
  run(ApiKey, MaxVersion, RequestFun, {ResponseFun, []}, State);
run(ApiKey, MaxVersion, {RequestFun, RequestParams}, {ResponseFun, ResponseParams}, State) when is_integer(ApiKey),
                                                                                                is_integer(MaxVersion),
                                                                                                is_function(RequestFun),
                                                                                                is_list(RequestParams),
                                                                                                (is_function(ResponseFun) orelse ResponseFun == undefined),
                                                                                                is_list(ResponseParams),
                                                                                                is_map(State) ->

  case api_version(ApiKey, State) of
    -1 ->
      {error, api_not_available};
    Version ->
      ApiVersion = check_version(Version, MaxVersion),
      Broker = maps:get(broker, State, first_broker),
      do_run(Broker,
             {call,
              {RequestFun, RequestParams},
              {ResponseFun, ResponseParams},
              State#{api_key => ApiKey,
                     api_version => ApiVersion}})
  end.

request(RequestMessage, #{api_key := ApiKey,
                          api_version := ApiVersion,
                          correlation_id := CorrelationId,
                          client_id := ClientId} = State) ->
  #{packet => <<
                ApiKey:16/signed,
                ApiVersion:16/signed,
                CorrelationId:32/signed,
                (encode_string(ClientId))/binary,
                RequestMessage/binary
              >>,
    state => maps:update(correlation_id, CorrelationId + 1, State)}.

response(<<CorrelationId:32/signed, Remainder/bytes>>, #{requests := Requests} = State) ->
  case orddict:find(CorrelationId, Requests) of
    {ok, #{from := From, handler := {ResponseHandler, ResponseHandlerParams}}} ->
      _ = gen_server:reply(From, erlang:apply(ResponseHandler, [Remainder|ResponseHandlerParams])),
      {ok, maps:update(requests, orddict:erase(CorrelationId, Requests), State)};
    error ->
      {error, request_not_found} %;
  end.

encode_string(undefined) ->
  <<-1:16/signed>>;
encode_string(Data) when is_binary(Data) ->
  <<(byte_size(Data)):16/signed, Data/binary>>.

encode_bytes(undefined) ->
  <<-1:32/signed>>;
encode_bytes(Data) ->
  <<(byte_size(Data)):32/signed, Data/binary>>.

encode_array(List) ->
  Len = length(List),
  Payload = << <<B/binary>> || B <- List>>,
  <<Len:32/signed, Payload/binary>>.

encode_boolean(true) -> <<1>>;
encode_boolean(_) -> <<0>>.

encode_varint(Int) ->
  iolist_to_binary(varint_encode(varint_e(Int))).
varint_e(Int) when (Int >= -(1 bsl ?VARINT_MAX_BITS)) and (Int < ((1 bsl ?VARINT_MAX_BITS) -1)) ->
  (Int bsl 1) bxor (Int bsr ?VARINT_MAX_BITS).
varint_encode(Int) when (Int bsr 7) =:= 0 ->
  [Int band 127];
varint_encode(Int) ->
  [128 + (Int band 127) | varint_encode(Int bsr 7)].

decode_varint(Bin) ->
  varint_d(varint_decode(Bin)).

varint_decode(Bin) -> varint_decode(Bin, 0, 0).
varint_decode(<<Tag:1, Value:7, Tail/binary>>, Acc, AccBits) when AccBits =< ?VARINT_MAX_BITS, Tag =:= 0 ->
  {(Value bsl AccBits) bor Acc, Tail};
varint_decode(<<Tag:1, Value:7, Tail/binary>>, Acc, AccBits) when AccBits =< ?VARINT_MAX_BITS, Tag =/= 0 ->
  varint_decode(Tail, (Value bsl AccBits) bor Acc, AccBits + 7).

varint_d({Int, TailBin}) ->
  {(Int bsr 1) bxor -(Int band 1), TailBin}.

encode_record_batch(Messages, Options) ->
  case lists:any(fun(T) ->
                     erlang:tuple_size(T) =:= 4 orelse
                     (
                      erlang:tuple_size(T) =:= 3 andalso
                      is_list(element(3, T))
                     )
                 end, Messages) of
    true ->
      encode_record_batch_v2(Messages, Options);
    false ->
      encode_record_batch_v01(Messages, 0, <<>>)
  end.

% RecordBatch =>
%   FirstOffset => int64          -- always 0
%   Length => int32
%   PartitionLeaderEpoch => int32 -- set by brocker so always -1
%   Magic => int8                 -- always 2
%   CRC => int32
%   Attributes => int16           -- 0 bor 0 bor (0 | 1 bsl 4) bor 0
%   LastOffsetDelta => int32
%   FirstTimestamp => int64
%   MaxTimestamp => int64
%   ProducerId => int64
%   ProducerEpoch => int16
%   FirstSequence => int32
%   Records => [Record]
%
% Record =>
%   Length => varint
%   Attributes => int8
%   TimestampDelta => varint
%   OffsetDelta => varint
%   KeyLen => varint
%   Key => data
%   ValueLen => varint
%   Value => data
%   Headers => [Header]
%
% Header => HeaderKey HeaderVal
%   HeaderKeyLen => varint
%   HeaderKey => string
%   HeaderValueLen => varint
%   HeaderValue => data
encode_record_batch_v2([FirstMessage|OtherMessages] = Messages, Options) ->
  FirstOffset = 0,
  PartitionLeaderEpoch = -1,
  Magic = 2,
  ProducerID = maps:get(producer_id, Options, -1),
  ProducerEpoch = maps:get(producer_epoch, Options, -1),
  Attributes = attributes(is_integer(ProducerID) andalso ProducerID >= 0),
  {FirstTimestamp, MaxTimestamp} = timestamps(FirstMessage, OtherMessages),
  FirstSequence = maps:get(first_sequence, Options, -1),
  LastOffsetDelta = length(OtherMessages),
  Records = encode_records(Messages, FirstOffset, FirstTimestamp),
  Body0 = <<
            Attributes:16/signed,
            LastOffsetDelta:32/signed,
            FirstTimestamp:64/signed,
            MaxTimestamp:64/signed,
            ProducerID:64/signed,
            ProducerEpoch:16/signed,
            FirstSequence:32/signed,
            Records/binary
          >>,
  CRC = erlang:crc32(Body0),
  Body1 = <<
            PartitionLeaderEpoch:32/signed,
            Magic:8/signed,
            CRC:32/signed,
            Body0/binary
          >>,
  <<
    FirstOffset:64/signed,
    (size(Body1)):32/signed,
    Body1/binary
  >>.

encode_records(Messages, Offset, Timestamp) ->
  encode_array(encode_records(Messages, Offset, Timestamp, [])).

encode_records([], _Offset, _Timestamp, Acc) ->
  lists:reverse(Acc);
encode_records([Message|Rest], Offset, Timestamp, Acc) ->
  encode_records(
    Rest,
    Offset + 1,
    Timestamp,
    [encode_record(Message, Offset, Timestamp)|Acc]).

encode_record({Key, Value, Timestamp, Headers}, Offset, BaseTimestamp) ->
  KeyB = bucs:to_binary(Key),
  ValueB = bucs:to_binary(Value),
  Message = <<
              0:8/signed,                                        % Attributes
              (encode_varint(Timestamp - BaseTimestamp))/binary, % TimestampDelta
              (encode_varint(Offset))/binary,                    % OffsetDelta
              (encode_varint(size(KeyB)))/binary,                % KeyLen
              KeyB/binary,                                       % Key
              (encode_varint(size(ValueB)))/binary,              % ValueLen
              ValueB/binary,                                     % Value
              (encode_headers(Headers))/binary
            >>,
  <<(encode_varint(size(Message)))/binary, % Length
    Message/binary>>;
encode_record({Key, Value, Timestamp}, Offset, BaseTimestamp) when is_integer(Timestamp) ->
  encode_record({Key, Value, Timestamp, []}, Offset, BaseTimestamp);
encode_record({Key, Value, Headers}, Offset, BaseTimestamp) when is_list(Headers) ->
  encode_record({Key, Value, BaseTimestamp, Headers}, Offset, BaseTimestamp);
encode_record({Key, Value}, Offset, BaseTimestamp) ->
  encode_record({Key, Value, BaseTimestamp, []}, Offset, BaseTimestamp).

encode_headers(Headers) ->
  encode_array([encode_header(Header) || Header <- Headers]).

encode_header({Key, Value}) ->
  KeyB = bucs:to_binary(Key),
  ValueB = bucs:to_binary(Value),
  <<
    (encode_varint(size(KeyB)))/binary,
    KeyB/binary,
    (encode_varint(size(ValueB)))/binary,
    ValueB/binary
  >>.

attributes(true) ->
  1 bsl 4;
attributes(false) ->
  0.

timestamps({_Key, _Value, Timestamp, _Headers}, OtherMessages) when is_integer(Timestamp) ->
  timestamps(Timestamp, Timestamp, OtherMessages);
timestamps({_Key, _Value, Timestamp}, OtherMessages) when is_integer(Timestamp) ->
  timestamps(Timestamp, Timestamp, OtherMessages);
timestamps(_Message, OtherMessages) ->
  Timestamp = kafe_utils:timestamp(),
  timestamps(Timestamp, Timestamp, OtherMessages).

timestamps(FirstTimestamp, MaxTimestamp, []) ->
  {FirstTimestamp, MaxTimestamp};
timestamps(FirstTimestamp, MaxTimestamp, [{_Key, _Value, Timestamp, _Headers}|Rest]) when is_integer(Timestamp) ->
  timestamps(FirstTimestamp, erlang:max(MaxTimestamp, Timestamp), Rest);
timestamps(FirstTimestamp, MaxTimestamp, [{_Key, _Value, Timestamp}|Rest]) when is_integer(Timestamp) ->
  timestamps(FirstTimestamp, erlang:max(MaxTimestamp, Timestamp), Rest);
timestamps(FirstTimestamp, MaxTimestamp, [_Message|Rest]) ->
  timestamps(FirstTimestamp, MaxTimestamp, Rest).

% Message Set:
% MessageSet => [Offset MessageSize Message]
%   Offset => int64
%   MessageSize => int32
%
% v0
% Message => Crc MagicByte Attributes Key Value
%   Crc => int32
%   MagicByte => int8
%   Attributes => int8
%   Key => bytes
%   Value => bytes
%
% v1 (supported since 0.10.0)
% Message => Crc MagicByte Attributes Timestamp Key Value
%   Crc => int32
%   MagicByte => int8
%   Attributes => int8
%   Timestamp => int64
%   Key => bytes
%   Value => bytes
encode_record_batch_v01([], _Offset, Acc) ->
  encode_bytes(Acc);
encode_record_batch_v01([{Key, Value, Timestamp}|Rest], Offset, Acc) ->
  Message = <<
              1:8/signed, % MagicByte
              0:8/signed, % Attributes
              Timestamp:64/signed, % Timestamp
              (kafe_protocol:encode_bytes(bucs:to_binary(Key)))/binary, % Key
              (kafe_protocol:encode_bytes(bucs:to_binary(Value)))/binary % Value
            >>,
  SignedMessage = <<(erlang:crc32(Message)):32/signed, Message/binary>>,
  encode_record_batch_v01(Rest,
                          Offset + 1,
                          <<Acc/binary,
                            Offset:64/signed,
                            (encode_bytes(SignedMessage))/binary>>);
encode_record_batch_v01([{Key, Value}|Rest], Offset, Acc) ->
  Message = <<
              0:8/signed, % MagicByte
              0:8/signed, % Attributes
              (kafe_protocol:encode_bytes(bucs:to_binary(Key)))/binary, % Key
              (kafe_protocol:encode_bytes(bucs:to_binary(Value)))/binary % Value
            >>,
  SignedMessage = <<(erlang:crc32(Message)):32/signed, Message/binary>>,
  encode_record_batch_v01(Rest,
                          Offset + 1,
                          <<Acc/binary,
                            Offset:64/signed,
                            (encode_bytes(SignedMessage))/binary>>).

% PRIVATE

do_run(first_broker, Request) ->
  case kafe_brokers:first_broker(false) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run(BrokerName, Request) when is_list(BrokerName) ->
  case kafe_brokers:broker_by_name(BrokerName) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run(BrokerID, Request) when is_atom(BrokerID) ->
  case kafe_brokers:broker_by_id(BrokerID) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run({host_and_port, Host, Port}, Request) ->
  case kafe_brokers:broker_by_host_and_port(Host, Port) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run({coordinator, GroupId}, Request) ->
  case kafe:group_coordinator(bucs:to_binary(GroupId)) of
    {ok, #{coordinator_host := Host,
           coordinator_port := Port,
           error_code := none}} ->
      case do_run({host_and_port, Host, Port}, Request) of
        {ok, #{error_code := not_coordinator_for_group}} ->
          retry_with_coordinator(GroupId, Request);
        {ok, [#{error_code := not_coordinator_for_group}]} ->
          retry_with_coordinator(GroupId, Request);
        {error, no_broker_found} ->
          retry_with_coordinator(GroupId, Request);
        Other ->
          Other
      end;
    _ ->
      {error, no_broker_found}
  end;
do_run(BrokerPID, Request) when is_pid(BrokerPID) ->
  case erlang:is_process_alive(BrokerPID) of
    true ->
      try
        Response = gen_server:call(BrokerPID, Request, ?TIMEOUT),
        _ = kafe_brokers:release_broker(BrokerPID),
        Response
      catch
        Type:Error ->
          _ = kafe_brokers:release_broker(BrokerPID),
          lager:error("Request error: ~p:~p", [Type, Error]),
          {error, Error}
      end;
    false ->
      _ = kafe_brokers:release_broker(BrokerPID),
      {error, broker_not_available}
  end.

api_version(ApiKey, State) ->
  case maps:get(api_version, State, undefined) of
    undefined ->
      case ApiKey of
        ?API_VERSIONS_REQUEST -> 0;
        _ -> kafe:api_version(ApiKey)
      end;
    V -> V
  end.

check_version(V, Max) when V > Max ->
  Max;
check_version(V, _) -> V.

retry_with_coordinator(GroupId, Request) ->
  case kafe_protocol_group_coordinator:run(GroupId, force) of
    {ok, #{coordinator_host := Host,
           coordinator_port := Port,
           error_code := none}} ->
      do_run({host_and_port, Host, Port}, Request);
    _ ->
      {error, no_broker_found}
  end.
