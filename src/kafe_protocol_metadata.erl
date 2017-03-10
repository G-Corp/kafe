% @hidden
-module(kafe_protocol_metadata).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 2).

-export([
         run/1,
         request/2,
         response/2
        ]).

run(Topics) ->
  kafe_protocol:run(
    ?METADATA_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request/2, [Topics]},
    fun ?MODULE:response/2).

% Metadata Request (Version: 0) => [topics]
% Metadata Request (Version: 1) => [topics]
% Metadata Request (Version: 2) => [topics]
request(TopicNames, #{api_version := ApiVersion} = State) ->
  kafe_protocol:request(
    encode_array_or_null(TopicNames, ApiVersion),
    State).

encode_array_or_null([], ApiVersion) when ApiVersion == ?V1;
                                          ApiVersion == ?V2 ->
  <<-1:32/signed>>;
encode_array_or_null(Array, _) ->
  <<(kafe_protocol:encode_array(
       [kafe_protocol:encode_string(bucs:to_binary(Element)) || Element <- Array]))/binary>>.

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
%       replicas => INT32
%       isr => INT32
%
% Metadata Response (Version: 1) => [brokers] controller_id [topic_metadata]
%   brokers => node_id host port rack
%     node_id => INT32
%     host => STRING
%     port => INT32
%     rack => NULLABLE_STRING
%   controller_id => INT32
%   topic_metadata => topic_error_code topic is_internal [partition_metadata]
%     topic_error_code => INT16
%     topic => STRING
%     is_internal => BOOLEAN
%     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
%       partition_error_code => INT16
%       partition_id => INT32
%       leader => INT32
%       replicas => INT32
%       isr => INT32
%
% Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topic_metadata]
%   brokers => node_id host port rack
%     node_id => INT32
%     host => STRING
%     port => INT32
%     rack => NULLABLE_STRING
%   cluster_id => NULLABLE_STRING
%   controller_id => INT32
%   topic_metadata => topic_error_code topic is_internal [partition_metadata]
%     topic_error_code => INT16
%     topic => STRING
%     is_internal => BOOLEAN
%     partition_metadata => partition_error_code partition_id leader [replicas] [isr]
%       partition_error_code => INT16
%       partition_id => INT32
%       leader => INT32
%       replicas => INT32
%       isr => INT32
response(<<NumberOfBrokers:32/signed,
           BrokerRemainder/binary>>,
         #{api_version := ApiVersion}) when ApiVersion == ?V0 ->
  {
   Brokers,
   <<
     NumberOfTopics:32/signed,
     TopicMetadataRemainder/binary
   >>
  } = brokers(NumberOfBrokers, BrokerRemainder, [], ApiVersion),
  {Topics, _} = topics(NumberOfTopics, TopicMetadataRemainder, [], ApiVersion),
  {ok, #{brokers => Brokers, topics => Topics}};
response(<<NumberOfBrokers:32/signed,
           BrokerRemainder/binary>>,
         #{api_version := ApiVersion}) when ApiVersion == ?V1 ->
  {
   Brokers,
   <<
     ControllerID:32/signed,
     NumberOfTopics:32/signed,
     TopicMetadataRemainder/binary
   >>
  } = brokers(NumberOfBrokers, BrokerRemainder, [], ApiVersion),
  {Topics, _} = topics(NumberOfTopics, TopicMetadataRemainder, [], ApiVersion),
  {ok, #{brokers => Brokers,
         controller_id => ControllerID,
         topics => Topics}};
response(<<NumberOfBrokers:32/signed,
           BrokerRemainder/binary>>,
         #{api_version := ApiVersion}) when ApiVersion == ?V2 ->
  {
   Brokers,
   <<
     ClusterIDLength:16/signed,
     Remainder/binary
   >>
  } = brokers(NumberOfBrokers, BrokerRemainder, [], ApiVersion),
  {ClusterID0,
   ControllerID0,
   NumberOfTopics0,
   TopicMetadataRemainder0} = if
                                ClusterIDLength == -1 ->
                                  <<
                                    ControllerID:32/signed,
                                    NumberOfTopics:32/signed,
                                    TopicMetadataRemainder/binary
                                  >> = Remainder,
                                  {<<>>,
                                   ControllerID,
                                   NumberOfTopics,
                                   TopicMetadataRemainder};
                                true ->
                                  <<
                                    ClusterID:ClusterIDLength/bytes,
                                    ControllerID:32/signed,
                                    NumberOfTopics:32/signed,
                                    TopicMetadataRemainder/binary
                                  >> = Remainder,
                                  {ClusterID,
                                   ControllerID,
                                   NumberOfTopics,
                                   TopicMetadataRemainder}
                              end,
  {Topics, _} = topics(NumberOfTopics0, TopicMetadataRemainder0, [], ApiVersion),
  {ok, #{brokers => Brokers,
         cluster_id => ClusterID0,
         controller_id => ControllerID0,
         topics => Topics}}.

% Private

brokers(0, Remainder, Acc, _) ->
  {Acc, Remainder};
brokers(
  N,
  <<
    NodeId:32/signed,
    HostLength:16/signed,
    Host:HostLength/bytes,
    Port:32/signed,
    RackLength:16/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V1;
                   ApiVersion == ?V2->
  {Rack0, Remainder0} = if
                          RackLength == -1 ->
                            {<<>>, Remainder};
                          true ->
                            <<Rack:RackLength/bytes, R/binary>> = Remainder,
                            {Rack, R}
                        end,
  brokers(N-1,
          Remainder0,
          [#{id => NodeId, host => Host, port => Port, rack => Rack0} | Acc],
          ApiVersion);
brokers(
  N,
  <<
    NodeId:32/signed,
    HostLength:16/signed,
    Host:HostLength/bytes,
    Port:32/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V0 ->
  brokers(N-1,
          Remainder,
          [#{id => NodeId, host => Host, port => Port} | Acc],
          ApiVersion).

topics(0, <<Remainder/binary>>, Acc, _) ->
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
  Acc,
  ApiVersion) when ApiVersion == ?V0 ->
  {Partitions, Remainder} = partitions(PartitionLength, PartitionsRemainder, [], ApiVersion),
  topics(N-1,
         Remainder,
         [#{error_code => kafe_error:code(ErrorCode),
            name => TopicName,
            partitions => Partitions} | Acc],
         ApiVersion);
topics(
  N,
  <<
    ErrorCode:16/signed,
    TopicNameLen:16/signed,
    TopicName:TopicNameLen/bytes,
    IsInternal:1/bytes,
    PartitionLength:32/signed,
    PartitionsRemainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V1;
                   ApiVersion == ?V2 ->
  {Partitions, Remainder} = partitions(PartitionLength, PartitionsRemainder, [], ApiVersion),
  topics(N-1,
         Remainder,
         [#{error_code => kafe_error:code(ErrorCode),
            name => TopicName,
            is_internal => not(IsInternal == <<0>>),
            partitions => Partitions} | Acc],
         ApiVersion).

partitions(0, <<Remainder/binary>>, Acc, _) ->
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
  Acc,
  ApiVersion) when ApiVersion == ?V0;
                   ApiVersion == ?V1;
                   ApiVersion == ?V2 ->
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
                isr => ISR} | Acc],
             ApiVersion).

isrs(0, Remainder, Acc) ->
  {Acc, Remainder};
isrs(N, <<InSyncReplica:32/signed, Remainder/binary>>, Acc) ->
  isrs(N-1, Remainder, [InSyncReplica | Acc]).

replicas(0, Remainder, Acc) ->
  {Acc, Remainder};
replicas(N, <<Replica:32/signed, Remainder/binary>>, Acc) ->
  replicas(N-1, Remainder, [Replica | Acc]).

