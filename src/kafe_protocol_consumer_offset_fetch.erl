% @hidden
-module(kafe_protocol_consumer_offset_fetch).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/3 % TODO /2
        ]).

run(ConsumerGroup, Options) ->
  Options1 = if
               Options =:= [] -> maps:keys(kafe:topics());
               true -> Options
             end,
  kafe_protocol:run(
    ?OFFSET_FETCH_REQUEST,
    {fun ?MODULE:request/3, [ConsumerGroup, Options1]},
    fun ?MODULE:response/3, % TODO /2
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
request(ConsumerGroup, Options, State) ->
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
response(<<NumberOfTopics:32/signed, Remainder/binary>>, _ApiVersion, _State) -> % TODO remove _ApiVersion
  {ok, response2(NumberOfTopics, Remainder)}.

% Private

topics(Options) ->
  topics(Options, <<(length(Options)):32/signed>>).

topics([], Result) -> Result;
topics([{TopicName, Partitions}|Rest], Result) when is_list(Partitions) ->
  topics(Rest,
         <<
           Result/binary,
           (kafe_protocol:encode_string(TopicName))/binary,
           (kafe_protocol:encode_array(
              [<<Partition:32/signed>> || Partition <- Partitions]
             ))/binary
         >>);
topics([{TopicName, Partition}|Rest], Result) when is_integer(Partition) ->
  topics([{TopicName, [Partition]}|Rest], Result);
topics([TopicName|Rest], Result) ->
  topics([{TopicName, maps:keys(maps:get(TopicName, kafe:topics(), #{}))}|Rest], Result).

response2(0, <<>>) ->
  [];
response2(N,
         <<
           TopicNameLength:16/signed,
           TopicName:TopicNameLength/bytes,
           NumberOfPartitions:32/signed,
           PartitionsRemainder/binary
         >>) ->
  {PartitionsOffset, Remainder} = partitions_offset(NumberOfPartitions, PartitionsRemainder, []),
  [#{name => TopicName, partitions_offset => PartitionsOffset} | response2(N - 1, Remainder)].

partitions_offset(0, Remainder, Acc) ->
  {Acc, Remainder};
partitions_offset(N,
                  <<
                    Partition:32/signed,
                    Offset:64/signed,
                    MetadataLength:16/signed,
                    Metadata:MetadataLength/bytes,
                    ErrorCode:16/signed,
                    Remainder/binary
                  >>,
                  Acc) ->
  partitions_offset(N - 1,
                    Remainder,
                    [#{partition => Partition,
                       offset => Offset,
                       metadata => Metadata,
                       error_code => kafe_error:code(ErrorCode)} | Acc]).

