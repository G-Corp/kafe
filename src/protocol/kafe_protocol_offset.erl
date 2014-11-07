-module(kafe_protocol_offset).

-define(OFFSET_REQUEST, 2).
-define(DEFAULT_PARTITION, 0).
-define(DEFAULT_TIME, -1).
-define(DEFAULT_MAX_SIZE, 65535).

-export([
         request/3,
         response/1
        ]).

request(ReplicaId, Topics, State) ->
  kafe_protocol:request(
    ?OFFSET_REQUEST, 
    <<ReplicaId:32/signed, (topics(Topics))/binary>>,
    State).

response(<<NumberOfTopics:32/signed, Remainder/binary>>) ->
  {ok, response(NumberOfTopics, Remainder)}.

% Private

topics(Topics) ->
  topics(Topics, <<(length(Topics)):32/signed>>).

topics([], Acc) -> Acc;
topics([{TopicName, Partitions} | T], Acc) ->
  topics(T,
         <<
           Acc/binary, 
           (kafe_protocol:encode_string(TopicName))/binary,
           (kafe_protocol:encode_array(
              [<<Partition:32/signed, FetchOffset:64/signed, MaxBytes:32/signed>> || 
               {Partition, FetchOffset, MaxBytes} <- Partitions]))/binary
         >>);
topics([TopicName | T], Acc) ->
  topics([{TopicName, [{?DEFAULT_PARTITION, ?DEFAULT_TIME, ?DEFAULT_MAX_SIZE}]} | T], Acc).

response(0, <<>>) ->
    [];
response(
  N, 
  <<
    TopicNameLength:16/signed, 
    TopicName:TopicNameLength/bytes, 
    NumberOfPartitions:32/signed, 
    PartitionsRemainder/binary
  >>) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionsRemainder, []),
  [#{name => TopicName, partitions => Partitions} | response(N - 1, Remainder)].

partitions(0, Remainder, Acc) ->
    {Acc, Remainder};
partitions(
  N,
  <<
    Partition:32/signed, 
    ErrorCode:16/signed, 
    NumberOfOffsets:32/signed, 
    Offsets:NumberOfOffsets/binary-unit:64,
    Remainder/binary
  >>, 
  Acc) ->
  partitions(N - 1, 
             Remainder, 
             [#{id => Partition, 
                error_code => kafe_error:code(ErrorCode), 
                offsets => [Offset || <<Offset:64/signed>> <= Offsets]} | Acc]).

