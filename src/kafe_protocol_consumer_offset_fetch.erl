% @hidden
-module(kafe_protocol_consumer_offset_fetch).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 1).

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
response(<<NumberOfTopics:32/signed, Remainder/binary>>, _State) ->
  {ok, response2(NumberOfTopics, Remainder)}.

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

