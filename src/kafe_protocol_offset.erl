% @hidden
-module(kafe_protocol_offset).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/3 % TODO /2
        ]).

run(ReplicaID, []) ->
  case maps:keys(kafe:topics()) of
    Topics when is_list(Topics), length(Topics) > 0 ->
      run(ReplicaID, maps:keys(kafe:topics()));
    _ ->
      {error, cant_retrieve_topics}
  end;
run(ReplicaID, Topics) ->
  case lists:flatten(
         maps:fold(
           fun
             (undefined, _, Acc) ->
               Acc;
             (BrokerID, TopicsForBroker, Acc) ->
               case kafe_protocol:run(
                      ?OFFSET_REQUEST,
                      {fun ?MODULE:request/3, [ReplicaID, TopicsForBroker]},
                      fun ?MODULE:response/3,
                      #{broker => BrokerID}) of
                 {ok, Result} ->
                   [Result|Acc];
                 _ ->
                   Acc
               end
           end, [], dispatch(Topics))) of
    [] ->
      {error, no_broker_found};
    OffsetsData ->
      {ok, maps:fold(
             fun(K, V, Acc) ->
                 [#{name => K, partitions => V}|Acc]
             end,
             [],
             lists:foldl(
               fun(#{name := Name, partitions := Partitions}, Acc) ->
                   case maps:get(Name, Acc, undefined) of
                     undefined ->
                       maps:put(Name, Partitions, Acc);
                     Data ->
                       maps:put(Name, Data ++ Partitions, Acc)
                   end
               end, #{}, OffsetsData))}
  end.

% Offsets Request (Version: 0) => replica_id [topics]
%   replica_id => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition timestamp max_num_offsets
%       partition => INT32
%       timestamp => INT64
%       max_num_offsets => INT32
%
% Offsets Request (Version: 1) => replica_id [topics]
%   replica_id => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition timestamp
%       partition => INT32
%       timestamp => INT64
request(ReplicaId, Topics, #{api_version := ApiVersion} = State) ->
  kafe_protocol:request(
    <<ReplicaId:32/signed, (topics(Topics, ApiVersion))/binary>>,
    State).

% Offsets Response (Version: 0) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code [offsets]
%       partition => INT32
%       error_code => INT16
%
% Offsets Response (Version: 1) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code timestamp offset
%       partition => INT32
%       error_code => INT16
%       timestamp => INT64
%       offset => INT64
response(<<NumberOfTopics:32/signed, Remainder/binary>>, _ApiVersion, #{api_version := ApiVersion}) ->
  {ok, response(NumberOfTopics, Remainder, ApiVersion)};
response(0, <<>>, _ApiVersion) ->
  [];
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionsRemainder/binary
  >>,
  ApiVersion) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionsRemainder, [], ApiVersion),
  [#{name => TopicName, partitions => Partitions} | response(N - 1, Remainder, ApiVersion)].

% Private

dispatch(Topics) ->
  dispatch(Topics, #{}).

dispatch([], Result) ->
  Result;
dispatch([{Topic, Partitions}|Rest], Result) ->
  dispatch(Rest,
           lists:foldl(fun
                         ({ID, _, _} = Partition, Acc) ->
                           Broker = kafe_brokers:broker_id_by_topic_and_partition(Topic, ID),
                           Topics = maps:get(Broker, Acc, []),
                           maps:put(Broker, [{Topic, [Partition]}|Topics], Acc);
                         ({ID, _} = Partition, Acc) ->
                           Broker = kafe_brokers:broker_id_by_topic_and_partition(Topic, ID),
                           Topics = maps:get(Broker, Acc, []),
                           maps:put(Broker, [{Topic, [Partition]}|Topics], Acc);
                         (Partition, Acc) ->
                           Broker = kafe_brokers:broker_id_by_topic_and_partition(Topic, Partition),
                           Topics = maps:get(Broker, Acc, []),
                           maps:put(Broker, [{Topic, [Partition]}|Topics], Acc)
                       end, Result, Partitions));
dispatch([Topic|Rest], Result) when is_binary(Topic) ->
  dispatch([{Topic, kafe:partitions(Topic)} | Rest], Result).

topics(Topics, ApiVersion) ->
  topics(Topics, <<(length(Topics)):32/signed>>, ApiVersion).

topics([], Acc, _) -> Acc;
topics([{TopicName, Partitions} | T], Acc, ApiVersion) when ApiVersion == ?V0 ->
  topics(T,
         <<
           Acc/binary,
           (kafe_protocol:encode_string(bucs:to_binary(TopicName)))/binary,
           (kafe_protocol:encode_array(
              [case P of
                 {Partition, Timestamp, MaxNumOffsets} ->
                   <<Partition:32/signed,
                     Timestamp:64/signed,
                     MaxNumOffsets:32/signed>>;
                 {Partition, Timestamp} ->
                   <<Partition:32/signed,
                     Timestamp:64/signed,
                     ?DEFAULT_OFFSET_MAX_NUM_OFFSETS:32/signed>>;
                 Partition ->
                   <<Partition:32/signed,
                     ?DEFAULT_OFFSET_TIMESTAMP:64/signed,
                     ?DEFAULT_OFFSET_MAX_NUM_OFFSETS:32/signed>>
               end || P <- Partitions]))/binary
         >>,
         ApiVersion);
topics([{TopicName, Partitions} | T], Acc, ApiVersion) when ApiVersion == ?V1 ->
  topics(T,
         <<
           Acc/binary,
           (kafe_protocol:encode_string(bucs:to_binary(TopicName)))/binary,
           (kafe_protocol:encode_array(
              [case P of
                 {Partition, Timestamp, _} ->
                   <<Partition:32/signed, Timestamp:64/signed>>;
                 {Partition, Timestamp} ->
                   <<Partition:32/signed, Timestamp:64/signed>>;
                 Partition ->
                   <<Partition:32/signed,
                     ?DEFAULT_OFFSET_TIMESTAMP:64/signed>>
               end || P <- Partitions]))/binary
         >>,
         ApiVersion);
topics([TopicName | T], Acc, ApiVersion) when ApiVersion == ?V0;
                                              ApiVersion == ?V1 ->
  topics([{TopicName, kafe:partitions(TopicName)} | T],
         Acc,
         ApiVersion).

partitions(0, Remainder, Acc, _) ->
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
  Acc,
  ApiVersion) when ApiVersion == ?V0 ->
  partitions(N - 1,
             Remainder,
             [#{id => Partition,
                error_code => kafe_error:code(ErrorCode),
                offsets => [Offset || <<Offset:64/signed>> <= Offsets]} | Acc],
             ApiVersion);
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    Timestamp:64/unsigned,
    Offset:64/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V1 ->
  partitions(N - 1,
             Remainder,
             [#{id => Partition,
                error_code => kafe_error:code(ErrorCode),
                timestamp => Timestamp,
                offset => Offset} | Acc],
             ApiVersion).

