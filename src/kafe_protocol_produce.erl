% @hidden
-module(kafe_protocol_produce).

-include("../include/kafe.hrl").

-export([
         run/3,
         request/4,
         response/2
        ]).

run(Topic, Message, Options) ->
  Partition = maps:get(partition, Options, ?DEFAULT_PRODUCE_PARTITION),
  case kafe:broker(Topic, Partition) of
    undefined -> {error, no_broker_found};
    Broker ->
      gen_server:call(Broker,
                      {call,
                       fun ?MODULE:request/4, [bucs:to_binary(Topic), Message, Options],
                       fun ?MODULE:response/2},
                      infinity)
  end.

%% Produce Request (Version: 0) => acks timeout [topic_data]
%%   acks => INT16
%%   timeout => INT32
%%   topic_data => topic [data]
%%     topic => STRING
%%     data => partition record_set
%%       partition => INT32
%%       record_set => BYTES
%% Options:
%%   * timeout :: integer()       (default: 5000)
%%   * required_acks :: integer() (default: -1)
%%   * partition :: integer()     (default: 0)
%%   * timestamp :: integer()     (default: now)
request(Topic, Message, Options, #{api_version := ApiVersion} = State) ->
  Timeout = maps:get(timeout, Options, ?DEFAULT_PRODUCE_SYNC_TIMEOUT),
  RequiredAcks = maps:get(required_acks,
                          Options,
                          ?DEFAULT_PRODUCE_REQUIRED_ACKS),
  {Key, Value} = if
    is_tuple(Message) -> Message;
    true -> {undefined, Message}
  end,
  Partition = maps:get(partition, Options, ?DEFAULT_PRODUCE_PARTITION),
  Msg = if
          ApiVersion == ?V2 ->
            Timestamp = maps:get(timestamp, Options, get_timestamp()),
            <<
              ApiVersion:8/signed,
              0:8/signed,
              Timestamp:64/signed,
              (kafe_protocol:encode_bytes(bucs:to_binary(Key)))/binary,
              (kafe_protocol:encode_bytes(bucs:to_binary(Value)))/binary
            >>;
          true ->
            <<
              ApiVersion:8/signed,
              0:8/signed,
              (kafe_protocol:encode_bytes(bucs:to_binary(Key)))/binary,
              (kafe_protocol:encode_bytes(bucs:to_binary(Value)))/binary
            >>
        end,
  SignedMsg = <<(erlang:crc32(Msg)):32/signed, Msg/binary>>,
  MessageSet = <<0:64/signed, (kafe_protocol:encode_bytes(SignedMsg))/binary>>,
  Encoded = <<
              1:32/signed,
              (kafe_protocol:encode_string(Topic))/binary,
              1:32/signed,
              Partition:32/signed,
              (kafe_protocol:encode_bytes(MessageSet))/binary
            >>,
  kafe_protocol:request(
    ?PRODUCE_REQUEST,
    <<RequiredAcks:16, Timeout:32, Encoded/binary>>,
    State,
    ApiVersion).

% Produce Request (Version: 1) => acks timeout [topic_data]
%   acks => INT16
%   timeout => INT32
%   topic_data => topic [data]
%     topic => STRING
%     data => partition record_set
%       partition => INT32
%       record_set => BYTES
response(<<NumberOfTopics:32/signed, Remainder/binary>>, ApiVersion) ->
  {ok, response(NumberOfTopics, Remainder, ApiVersion)}.

% Private

% v0
response(0, _, ApiVersion) when ApiVersion == ?V0 ->
  [];
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary
  >>,
  ApiVersion) when ApiVersion == ?V0 ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionRemainder, [], ApiVersion),
  [#{name => TopicName,
     partitions => Partitions} | response(N - 1, Remainder, 0)];
% v1 & v2
response(N, Remainder, ApiVersion) when ApiVersion == ?V1;
                                        ApiVersion == ?V2 ->
  {Topics, <<ThrottleTime:32/signed, _/binary>>} = response(N, Remainder, ApiVersion, []),
  #{topics => Topics,
    throttle_time => ThrottleTime}.

response(0, Remainder, ApiVersion, Acc) when ApiVersion == ?V1;
                                             ApiVersion == ?V2 ->
  {Acc, Remainder};
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary
  >>,
  ApiVersion,
  Acc) when ApiVersion == ?V1;
            ApiVersion == ?V2 ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionRemainder, [], ApiVersion),
  response(N - 1, Remainder, ApiVersion, [#{name => TopicName,
                                            partitions => Partitions} | Acc]).

% v0 & v1
partitions(0, Remainder, Acc, ApiVersion) when ApiVersion == ?V0;
                                               ApiVersion == ?V1 ->
  {Acc, Remainder};
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    Offset:64/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V0;
                   ApiVersion == ?V1 ->
  partitions(N - 1,
             Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                offset => Offset} | Acc], ApiVersion);
partitions(0, Remainder, Acc, ApiVersion) when ApiVersion == ?V2 ->
  {Acc, Remainder};
% v2
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    Offset:64/signed,
    Timestamp:64/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V2 ->
  partitions(N - 1,
             Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                offset => Offset,
                timestamp => Timestamp} | Acc], ApiVersion).

get_timestamp() ->
  {Mega,Sec,Micro} = erlang:timestamp(),
  (Mega*1000000+Sec)*1000000+Micro.

