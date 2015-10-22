% @hidden
-module(kafe_protocol_metadata).

-include("../include/kafe.hrl").

-export([
         run/1,
         request/2,
         response/1
        ]).

run(Topics) ->
  case kafe:first_broker() of
    undefined -> {error, no_broker_found};
    Broker ->
      gen_server:call(Broker,
                      {call, 
                       fun ?MODULE:request/2, [Topics],
                       fun ?MODULE:response/1},
                      infinity)
  end.

request(TopicNames, State) ->
  kafe_protocol:request(
    ?METADATA_REQUEST, 
    <<(kafe_protocol:encode_array(
         [kafe_protocol:encode_string(Name) || 
          Name <- TopicNames]))/binary>>,
    State).

response(<<NumberOfBrokers:32/signed, BrokerRemainder/binary>>) ->
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

