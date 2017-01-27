% @hidden
-module(kafe_protocol_fetch).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/3,
         request/4,
         response/3 % TODO /2
        ]).

run(ReplicaID, TopicName, Options) ->
  {Partition, Offset} = case {maps:get(partition, Options, undefined),
                              maps:get(offset, Options, undefined)} of
                          {undefined, undefined} ->
                            kafe:max_offset(TopicName);
                          {Partition1, undefined} ->
                            kafe:max_offset(TopicName, Partition1);
                          {undefined, Offset1} ->
                            kafe:partition_for_offset(TopicName, Offset1);
                          R -> R
                        end,
  Options1 = Options#{partition => Partition,
                      offset => Offset},
  kafe_protocol:run(
    ?FETCH_REQUEST,
    {fun ?MODULE:request/4, [ReplicaID, bucs:to_binary(TopicName), Options1]},
    fun ?MODULE:response/3,
    #{broker => {topic_and_partition, TopicName, Partition}}).

% Fetch Request (Version: 0) => replica_id max_wait_time min_bytes [topics]
%   replica_id => INT32
%   max_wait_time => INT32
%   min_bytes => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition fetch_offset max_bytes
%       partition => INT32
%       fetch_offset => INT64
%       max_bytes => INT32
%
% Fetch Request (Version: 1) => replica_id max_wait_time min_bytes [topics]
%   replica_id => INT32
%   max_wait_time => INT32
%   min_bytes => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition fetch_offset max_bytes
%       partition => INT32
%       fetch_offset => INT64
%       max_bytes => INT32
%
% Fetch Request (Version: 2) => replica_id max_wait_time min_bytes [topics]
%   replica_id => INT32
%   max_wait_time => INT32
%   min_bytes => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition fetch_offset max_bytes
%       partition => INT32
%       fetch_offset => INT64
%       max_bytes => INT32
%
% Fetch Request (Version: 3) => replica_id max_wait_time min_bytes max_bytes [topics]
%   replica_id => INT32
%   max_wait_time => INT32
%   min_bytes => INT32
%   max_bytes => INT32
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition fetch_offset max_bytes
%       partition => INT32
%       fetch_offset => INT64
%       max_bytes => INT32
request(ReplicaID, TopicName, Options, #{api_version := ApiVersion} = State) when ApiVersion == ?V0;
                                                                                  ApiVersion == ?V1;
                                                                                  ApiVersion == ?V2 ->
  Partition = maps:get(partition, Options, ?DEFAULT_FETCH_PARTITION),
  Offset = maps:get(offset, Options),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  kafe_protocol:request(
    <<ReplicaID:32/signed,
      MaxWaitTime:32/signed,
      MinBytes:32/signed,
      1:32/signed,
      (kafe_protocol:encode_string(TopicName))/binary,
      1:32/signed,
      Partition:32/signed,
      Offset:64/signed,
      MaxBytes:32/signed>>,
    State);
request(ReplicaID, TopicName, Options, #{api_version := ApiVersion} = State) when ApiVersion == ?V3 ->
  Partition = maps:get(partition, Options, ?DEFAULT_FETCH_PARTITION),
  Offset = maps:get(offset, Options),
  MaxBytes = maps:get(max_bytes, Options, ?DEFAULT_FETCH_MAX_BYTES),
  MinBytes = maps:get(min_bytes, Options, ?DEFAULT_FETCH_MIN_BYTES),
  ResponseMaxBytes = maps:get(response_max_bytes, Options, MaxBytes),
  MaxWaitTime = maps:get(max_wait_time, Options, ?DEFAULT_FETCH_MAX_WAIT_TIME),
  kafe_protocol:request(
    <<ReplicaID:32/signed,
      MaxWaitTime:32/signed,
      MinBytes:32/signed,
      ResponseMaxBytes:32/signed,
      1:32/signed,
      (kafe_protocol:encode_string(TopicName))/binary,
      1:32/signed,
      Partition:32/signed,
      Offset:64/signed,
      MaxBytes:32/signed>>,
    State).

% Fetch Response (Version: 0) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code high_watermark record_set
%       partition => INT32
%       error_code => INT16
%       high_watermark => INT64
%       record_set => BYTES
%
% Fetch Response (Version: 1) => throttle_time_ms [responses]
%   throttle_time_ms => INT32
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code high_watermark record_set
%       partition => INT32
%       error_code => INT16
%       high_watermark => INT64
%       record_set => BYTES
%
% Fetch Response (Version: 2) => throttle_time_ms [responses]
%   throttle_time_ms => INT32
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code high_watermark record_set
%       partition => INT32
%       error_code => INT16
%       high_watermark => INT64
%       record_set => BYTES
%
% Fetch Response (Version: 3) => throttle_time_ms [responses]
%   throttle_time_ms => INT32
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code high_watermark record_set
%       partition => INT32
%       error_code => INT16
%       high_watermark => INT64
%       record_set => BYTES
response(<<NumberOfTopics:32/signed,
           Remainder/binary>>,
         _ApiVersion,
         #{api_version := ApiVersion}) when ApiVersion == ?V0 -> % TODO remove _ApiVersion
  topics(NumberOfTopics, Remainder, []);
response(<<ThrottleTime:32/signed,
           NumberOfTopics:32/signed,
           Remainder/binary>>,
         _ApiVersion,
         #{api_version := ApiVersion}) when ApiVersion == ?V1;
                                            ApiVersion == ?V2;
                                            ApiVersion == ?V3 -> % TODO remove _ApiVersion
  case topics(NumberOfTopics, Remainder, []) of
    {ok, Response} ->
      {ok, #{topics => Response,
             throttle_time => ThrottleTime}};
    Error ->
      Error
  end;
response(_, _, _) ->
  {error, incomplete_data}.

% Private

topics(0, _, Result) ->
  {ok, Result};
topics(
  N,
  <<TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary>>,
  Acc) ->
  case partitions(NumberOfPartitions, PartitionRemainder, []) of
    {ok, Partitions, Remainder} ->
      topics(N - 1, Remainder, [#{name => TopicName,
                                  partitions => Partitions}|Acc]);
    Error ->
      Error
  end;
topics(_, _, _) ->
  {error, incomplete_data}.

partitions(0, Remainder, Acc) ->
  {ok, Acc, Remainder};
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
                high_watermark_offset => HighwaterMarkOffset,
                messages => message(MessageSet)} | Acc]);
partitions(_, _, _) ->
  {error, incomplete_data}.

message(Data) ->
  message(Data, []).

message(<<Offset:64/signed,
          MessageSize:32/signed,
          Message:MessageSize/binary,
          Remainder/binary>>, Acc) ->
  case Message of
    <<Crc:32/signed,
      0:8/signed,
      Attibutes:8/signed,
      MessageRemainder/binary>> ->
      {Key, MessageRemainder1} = get_kv(MessageRemainder),
      {Value, <<>>} = get_kv(MessageRemainder1),
      message(Remainder, [#{offset => Offset,
                            crc => Crc,
                            magic_byte => 0,
                            attributes => Attibutes,
                            key => Key,
                            value => Value}|Acc]);
    <<Crc:32/signed,
      1:8/signed,
      Attibutes:8/signed,
      Timestamp:64/signed,
      MessageRemainder/binary>> ->
      {Key, MessageRemainder1} = get_kv(MessageRemainder),
      {Value, <<>>} = get_kv(MessageRemainder1),
      message(Remainder, [#{offset => Offset,
                            crc => Crc,
                            magic_byte => 1,
                            attributes => Attibutes,
                            timestamp => Timestamp,
                            key => Key,
                            value => Value}|Acc])
  end;
message(_, Acc) ->
  lists:reverse(Acc).

get_kv(<<KVSize:32/signed, Remainder/binary>>) when KVSize =:= -1 ->
  {<<>>, Remainder};
get_kv(<<KVSize:32/signed, KV:KVSize/binary, Remainder/binary>>) ->
  {KV, Remainder}.

