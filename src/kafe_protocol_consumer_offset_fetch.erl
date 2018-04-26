% @hidden
-module(kafe_protocol_consumer_offset_fetch).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 3).

-export([
         run/2,
         request/3,
         response/2
        ]).

run(ConsumerGroup, Options) ->
  Options1 = if
               Options =:= [] -> maps:keys(kafe:topics());
               true -> Options
             end,
  kafe_protocol:run(
    ?OFFSET_FETCH_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request/3, [ConsumerGroup, Options1]},
    fun ?MODULE:response/2,
    #{broker => {coordinator, ConsumerGroup}}).

% OffsetFetch Request (Version: 0) => group_id [topics]
%   group_id => STRING
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition
%       partition => INT32
%
% OffsetFetch Request (Version: 1) => group_id [topics]
%   group_id => STRING
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition
%       partition => INT32
%
% OffsetFetch Request (Version: 2) => group_id [topics]
%   group_id => STRING
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition
%       partition => INT32
%
% OffsetFetch Request (Version: 3) => group_id [topics]
%   group_id => STRING
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition
%       partition => INT32
request(ConsumerGroup, Options, #{api_version := ApiVersion} = State) when ApiVersion == ?V0;
                                                                           ApiVersion == ?V1;
                                                                           ApiVersion == ?V2;
                                                                           ApiVersion == ?V3 ->
  kafe_protocol:request(
    <<(kafe_protocol:encode_string(ConsumerGroup))/binary, (topics(Options))/binary>>,
    State).

% OffsetFetch Response (Version: 0) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition offset metadata error_code
%       partition => INT32
%       offset => INT64
%       metadata => NULLABLE_STRING
%       error_code => INT16
%
% OffsetFetch Response (Version: 1) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition offset metadata error_code
%       partition => INT32
%       offset => INT64
%       metadata => NULLABLE_STRING
%       error_code => INT16
%
% OffsetFetch Response (Version: 2) => [responses] error_code
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition offset metadata error_code
%       partition => INT32
%       offset => INT64
%       metadata => NULLABLE_STRING
%       error_code => INT16
%   error_code => INT16
%
% OffsetFetch Response (Version: 3) => throttle_time_ms [responses] error_code
%   throttle_time_ms => INT32
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition offset metadata error_code
%       partition => INT32
%       offset => INT64
%       metadata => NULLABLE_STRING
%       error_code => INT16
%   error_code => INT16
response(<<NumberOfTopics:32/signed, Remainder/binary>>, #{api_version := ApiVersion}) when ApiVersion == ?V0;
                                                                                            ApiVersion == ?V1 ->
  {Topics, _Remainder} = response2(NumberOfTopics, Remainder),
  {ok, Topics};
response(<<NumberOfTopics:32/signed, Remainder/binary>>, #{api_version := ApiVersion}) when ApiVersion == ?V2 ->
  {Topics, <<ErrorCode:16/signed>>} = response2(NumberOfTopics, Remainder),
  {ok, #{topics => Topics,
         error_code => kafe_error:code(ErrorCode)}};
response(<<ThrottleTimeMs:32/signed, NumberOfTopics:32/signed, Remainder/binary>>, #{api_version := ApiVersion}) when ApiVersion == ?V3 ->
  {Topics, <<ErrorCode:16/signed>>} = response2(NumberOfTopics, Remainder),
  {ok, #{throttle_time => ThrottleTimeMs,
         topics => Topics,
         error_code => kafe_error:code(ErrorCode)}}.

% Private

topics(Options) ->
  topics(Options, []).

topics([], Acc) ->
  kafe_protocol:encode_array(lists:reverse(Acc));
topics([{TopicName, Partitions}|Rest], Acc) when is_list(Partitions) ->
  topics(Rest,
         [<<
            (kafe_protocol:encode_string(TopicName))/binary,
            (kafe_protocol:encode_array(
               [<<Partition:32/signed>> || Partition <- Partitions]
              ))/binary
          >> | Acc]);
topics([{TopicName, Partition}|Rest], Acc) when is_integer(Partition) ->
  topics([{TopicName, [Partition]}|Rest], Acc);
topics([TopicName|Rest], Acc) ->
  topics([{TopicName, maps:keys(maps:get(TopicName, kafe:topics(), #{}))}|Rest], Acc).

response2(N, Data) ->
  response2(N, Data, []).

response2(0, Remain, Acc) ->
  {Acc, Remain};
response2(N,
          <<
            TopicNameLength:16/signed,
            TopicName:TopicNameLength/bytes,
            NumberOfPartitions:32/signed,
            PartitionsRemainder/binary
          >>,
          Acc) ->
  {PartitionsOffset, Remainder} = partitions_offset(NumberOfPartitions, PartitionsRemainder, []),
  response2(
    N - 1,
    Remainder,
    [#{name => TopicName, partitions_offset => PartitionsOffset} | Acc]).

partitions_offset(0, Remainder, Acc) ->
  {lists:reverse(Acc), Remainder};
partitions_offset(N,
                  <<
                    Partition:32/signed,
                    Offset:64/signed,
                    MetadataLength:16/signed,
                    MDRemainder/binary
                  >>,
                  Acc) ->
  {Metadata, ErrorCode, Remainder} = case MetadataLength of
    -1 ->
      <<EC:16/signed, R/binary>> = MDRemainder,
      {<<>>, EC, R};
    L ->
      <<MD:L/bytes, EC:16/signed, R/binary>> = MDRemainder,
      {MD, EC, R}
  end,
  partitions_offset(N - 1,
                    Remainder,
                    [#{partition => Partition,
                       offset => Offset,
                       metadata => Metadata,
                       error_code => kafe_error:code(ErrorCode)} | Acc]).
