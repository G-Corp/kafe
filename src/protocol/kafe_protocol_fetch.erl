-module(kafe_protocol_fetch).

-include("../include/kafe.hrl").

-export([
         request/4,
         response/1
        ]).

%% Options:
%%   * timeout :: integer()       (default: 5000)
%%   * required_acks :: integer() (default: 0)
%%   * partition :: integer()     (default: 0)
request(ReplicaID, TopicName, Options, #{offset := LastOffset} = State) ->
  Partition = maps:get(partition, Options, ?DEFAULT_FETCH_PARTITION),
  Offset = maps:get(offset, Options, LastOffset),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  kafe_protocol:request(
    ?FETCH_REQUEST,
    <<ReplicaID:32/signed,
      MaxWaitTime:32/signed,
      MinBytes:32/signed,
      1:32/signed,
      (kafe_protocol:encode_string(TopicName))/binary,
      1:32/signed,
      Partition:32/signed,
      Offset:64/signed,
      MaxBytes:32/signed>>,
    maps:update(offset, Offset + 1, State)).

response(<<NumberOfTopics:32/signed, Remainder/binary>>) ->
  {ok, response(NumberOfTopics, Remainder)}.

% Private

response(0, _) ->
    [];
response(
  N, 
  <<TopicNameLength:16/signed, 
    TopicName:TopicNameLength/bytes, 
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary>>) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionRemainder, []),
  [#{name => TopicName, 
     partitions => Partitions} | response(N - 1, Remainder)].

partitions(0, Remainder, Acc) ->
  {Acc, Remainder};
partitions(
  N, 
  <<Partition:32/signed,
    ErrorCode:16/signed,
    HighwaterMarkOffset:64/signed,
    MessageSetSize:32/signed,
    MessageSet:MessageSetSize/binary,
    Remainder/binary>>,
  Acc) ->
  partitions(N - 1, Remainder, 
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                high_watermaker_offset => HighwaterMarkOffset,
                %message => <<"...">>} | Acc]).
                message => message(MessageSet)} | Acc]).

message(<<>>) ->
  #{};
message(<<Offset:64/signed,
          MessageSize:32/signed,
          Message:MessageSize/binary,
          _Remainder/binary>>) ->
  <<Crc:32/signed,
    MagicByte:8/signed,
    Attibutes:8/signed,
    MessageRemainder/binary>> = Message,
   {Key, MessageRemainder1} = get_kv(MessageRemainder),
   {Value, <<>>} = get_kv(MessageRemainder1),
   #{offset => Offset,
     crc => Crc,
     magic_bytes => MagicByte,
     attributes => Attibutes,
     key => Key,
     value => Value}.

get_kv(<<KVSize:32/signed, Remainder/binary>>) when KVSize =:= -1 ->
  {<<>>, Remainder};
get_kv(<<KVSize:32/signed, KV:KVSize/binary, Remainder/binary>>) ->
  {KV, Remainder}.

