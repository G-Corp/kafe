% @hidden
-module(kafe_protocol_consumer_offset_commit).

-include("../include/kafe.hrl").

-export([
         run_v0/2,
         run_v1/4,
         run_v2/5,
         request_v0/3,
         request_v1/5,
         request_v2/6,
         response/2
        ]).

run_v0(ConsumerGroup, Topics) ->
  case kafe:consumer_metadata(ConsumerGroup) of
    {ok, #{coordinator_host := BrokerName}} ->
      gen_server:call(kafe:broker_by_name(BrokerName),
                      {call,
                       fun ?MODULE:request_v0/3, [ConsumerGroup, Topics],
                       fun ?MODULE:response/2},
                      infinity);
    E -> E
  end.

run_v1(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, Topics) ->
  case kafe:consumer_metadata(ConsumerGroup) of
    {ok, #{coordinator_host := BrokerName}} ->
      gen_server:call(kafe:broker_by_name(BrokerName),
                      {call,
                       fun ?MODULE:request_v1/5, [ConsumerGroup,
                                                  ConsumerGroupGenerationId,
                                                  ConsumerId,
                                                  Topics],
                       fun ?MODULE:response/2},
                      infinity);
    E -> E
  end.

run_v2(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, Topics) ->
  case kafe:consumer_metadata(ConsumerGroup) of
    {ok, #{coordinator_host := BrokerName}} ->
      gen_server:call(kafe:broker_by_name(BrokerName),
                      {call,
                       fun ?MODULE:request_v2/6, [ConsumerGroup,
                                                  ConsumerGroupGenerationId,
                                                  ConsumerId,
                                                  RetentionTime,
                                                  Topics],
                       fun ?MODULE:response/2},
                      infinity);
    E -> E
  end.

request_v0(ConsumerGroup, Topics, State) ->
  kafe_protocol:request(
    ?OFFSET_COMMIT_REQUEST,
    <<
      (kafe_protocol:encode_string(ConsumerGroup))/binary,
      (topics_v0_v2(Topics))/binary
    >>,
    State,
    ?V0).

request_v1(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, Topics, State) ->
  kafe_protocol:request(
    ?OFFSET_COMMIT_REQUEST,
    <<
      (kafe_protocol:encode_string(ConsumerGroup))/binary,
      ConsumerGroupGenerationId:32/signed,
      (kafe_protocol:encode_string(ConsumerId))/binary,
      (topics_v1(Topics))/binary
    >>,
    State,
    ?V1).

request_v2(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, Topics, State) ->
  kafe_protocol:request(
    ?OFFSET_COMMIT_REQUEST,
    <<
      (kafe_protocol:encode_string(ConsumerGroup))/binary,
      ConsumerGroupGenerationId:32/signed,
      (kafe_protocol:encode_string(ConsumerId))/binary,
      RetentionTime:64/signed,
      (topics_v0_v2(Topics))/binary
    >>,
    State,
    ?V2).

response(<<NumberOfTopics:32/signed, Remainder/binary>>, _ApiVersion) ->
  {ok, response2(NumberOfTopics, Remainder)}.

% Private

topics_v0_v2(Topics) ->
  topics_v0_v2(Topics, <<(length(Topics)):32/signed>>).

topics_v0_v2([], Acc) -> Acc;
topics_v0_v2([{TopicName, Partitions}|Rest], Acc) ->
  topics_v0_v2(Rest,
               <<
                 Acc/binary,
                 (kafe_protocol:encode_string(TopicName))/binary,
                 (kafe_protocol:encode_array(
                    [<<Partition:32/signed, Offset:64/signed, (kafe_protocol:encode_string(Metadata))/binary>> ||
                     {Partition, Offset, Metadata} <- Partitions]
                   ))/binary
               >>).

topics_v1(Topics) ->
  topics_v1(Topics, <<(length(Topics)):32/signed>>).

topics_v1([], Acc) -> Acc;
topics_v1([{TopicName, Partitions}|Rest], Acc) ->
  topics_v1(Rest,
               <<
                 Acc/binary,
                 (kafe_protocol:encode_string(TopicName))/binary,
                 (kafe_protocol:encode_array(
                    [<<Partition:32/signed,
                       Offset:64/signed,
                       Timestamp:64/signed,
                       (kafe_protocol:encode_string(Metadata))/binary>> ||
                     {Partition, Offset, Timestamp, Metadata} <- Partitions]
                   ))/binary
               >>).

response2(0, <<>>) ->
  [];
response2(N,
         <<
           TopicNameLength:16/signed,
           TopicName:TopicNameLength/bytes,
           NumberOfPartitions:32/signed,
           PartitionsRemainder/binary
         >>) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionsRemainder, []),
  [#{name => TopicName, partitions => Partitions} | response2(N - 1, Remainder)].

partitions(0, Remainder, Acc) ->
  {Acc, Remainder};
partitions(N,
           <<
             Partition:32/signed,
             ErrorCode:16/signed,
             Remainder/binary
           >>,
           Acc) ->
  partitions(N - 1,
             Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode)} | Acc]).


