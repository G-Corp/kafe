% @hidden
-module(kafe_protocol_produce).

-include("../include/kafe.hrl").

-export([
         run/3,
         request/4,
         response/1
        ]).

run(Topic, Message, Options) ->
  Partition = maps:get(partition, Options, ?DEFAULT_PRODUCE_PARTITION),
  case kafe:broker(Topic, Partition) of
    undefined -> {error, no_broker_found};
    Broker ->
      gen_server:call(Broker,
                      {call,
                       fun ?MODULE:request/4, [Topic, Message, Options],
                       fun ?MODULE:response/1},
                      infinity)
  end.

%% Options:
%%   * timeout :: integer()       (default: 5000)
%%   * required_acks :: integer() (default: 0)
%%   * partition :: integer()     (default: 0)
request(Topic, Message, Options, #{api_version := Magic} = State) ->
  Timeout = maps:get(timeout, Options, ?DEFAULT_PRODUCE_SYNC_TIMEOUT),
  RequiredAcks = maps:get(required_acks,
                          Options,
                          ?DEFAULT_PRODUCE_REQUIRED_ACKS) bor ?BITMASK_REQUIRE_ACK,
  {Key, Value} = if
    is_tuple(Message) -> Message;
    true -> {undefined, Message}
  end,
  Partition = maps:get(partition, Options, ?DEFAULT_PRODUCE_PARTITION),
  Msg = <<
          Magic:8/signed,
          0:8/signed,
          (kafe_protocol:encode_bytes(Key))/binary,
          (kafe_protocol:encode_bytes(Value))/binary
        >>,
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
    State).

response(<<NumberOfTopics:32/signed, Remainder/binary>>) ->
  {ok, response(NumberOfTopics, Remainder)}.

% Private

response(0, _) ->
    [];
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary
  >>) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionRemainder, []),
  [#{name => TopicName,
     partitions => Partitions} | response(N - 1, Remainder)].

partitions(0, Remainder, Acc) ->
  {Acc, Remainder};
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    Offset:64/signed,
    Remainder/binary
  >>,
  Acc) ->
  partitions(N - 1,
             Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                offset => Offset} | Acc]).

