% @hidden
-module(kafe_protocol_metadata).

-include("../include/kafe.hrl").

-export([
         run/1,
         request/2,
         response/2
        ]).

run(Topics) ->
  case kafe:first_broker() of
    undefined -> {error, no_broker_found};
    Broker ->
      gen_server:call(Broker,
                      {call,
                       fun ?MODULE:request/2, [Topics],
                       fun ?MODULE:response/2},
                      infinity)
  end.

% Metadata Request (Version: 0) => [topics]
request(TopicNames, State) ->
  kafe_protocol:request(
    ?METADATA_REQUEST,
    <<(kafe_protocol:encode_array(
         [kafe_protocol:encode_string(bucs:to_binary(Name)) || Name <- TopicNames]))/binary>>,
    State).

% Metadata Response (Version: 0) => [brokers] [topic_metadata]
%   brokers => node_id host port
%     node_id => INT32
%     host => STRING
%     port => INT32
%   topic_metadata => topic_error_code topic [partition_metadata]
%     topic_error_code => INT16
%     topic => STRING
%     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
%       partition_error_code => INT16
%       partition_id => INT32
%       leader => INT32
response(<<NumberOfBrokers:32/signed, BrokerRemainder/binary>>, _ApiVersion) ->
  {
   Brokers,
   <<NumberOfTopics:32/signed, TopicMetadataRemainder/binary>>
  } = brokers(NumberOfBrokers, BrokerRemainder, []),
  {Topics, _} = topics(NumberOfTopics, TopicMetadataRemainder, []),
  {ok, #{brokers => Brokers, topics => Topics}}.

% Private

brokers(0, Remainder, Acc) ->
  {Acc, Remainder};
brokers(
  N,
  <<
    NodeId:32/signed,
    HostLength:16/signed,
    Host:HostLength/bytes,
    Port:32/signed,
    Remainder/binary
  >>, Acc) ->
  brokers(N-1, Remainder, [#{id => NodeId, host => Host, port => Port} | Acc]).


topics(0, <<Remainder/binary>>, Acc) ->
  {Acc, Remainder};
topics(
  N,
  <<
    ErrorCode:16/signed,
    TopicNameLen:16/signed,
    TopicName:TopicNameLen/bytes,
    PartitionLength:32/signed,
    PartitionsRemainder/binary
  >>,
  Acc) ->

  {Partitions, Remainder} = partitions(PartitionLength, PartitionsRemainder, []),
  topics(N-1,
         Remainder,
         [#{error_code => kafe_error:code(ErrorCode),
            name => TopicName,
            partitions => Partitions} | Acc]).

partitions(0, <<Remainder/binary>>, Acc) ->
  {Acc, Remainder};
partitions(
  N,
  <<
    ErrorCode:16/signed,
    Id:32/signed,
    Leader:32/signed,
    NumberOfReplicas:32/signed,
    ReplicasRemainder/binary
  >>,
  Acc) ->
  {
   Replicas,
   <<NumberOfISR:32/signed, ISRRemainder/binary>>
  } = replicas(NumberOfReplicas, ReplicasRemainder, []),
  {ISR, Remainder} = isrs(NumberOfISR, ISRRemainder, []),
  partitions(N-1,
             Remainder,
             [#{error_code => kafe_error:code(ErrorCode),
                id => Id,
                leader => Leader,
                replicas => Replicas,
                isr => ISR} | Acc]).

isrs(0, Remainder, Acc) ->
  {Acc, Remainder};
isrs(N, <<InSyncReplica:32/signed, Remainder/binary>>, Acc) ->
  isrs(N-1, Remainder, [InSyncReplica | Acc]).

replicas(0, Remainder, Acc) ->
  {Acc, Remainder};
replicas(N, <<Replica:32/signed, Remainder/binary>>, Acc) ->
  replicas(N-1, Remainder, [Replica | Acc]).

