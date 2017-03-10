% @hidden
-module(kafe_protocol_consumer_offset_commit).

-include("../include/kafe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-define(MAX_VERSION, 2).

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
  kafe_protocol:run(
    ?OFFSET_COMMIT_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request_v0/3, [ConsumerGroup, Topics]},
    fun ?MODULE:response/2,
    #{api_version => 0,
      broker => {coordinator, ConsumerGroup}}).

run_v1(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, Topics) ->
  kafe_protocol:run(
    ?OFFSET_COMMIT_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request_v1/5, [ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, Topics]},
    fun ?MODULE:response/2,
    #{api_version => 1,
      broker => {coordinator, ConsumerGroup}}).

run_v2(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, Topics) ->
  kafe_protocol:run(
    ?OFFSET_COMMIT_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request_v2/6, [ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, Topics]},
    fun ?MODULE:response/2,
    #{api_version => 2,
      broker => {coordinator, ConsumerGroup}}).

% OffsetCommit Request (Version: 0) => group_id [topics]
%   group_id => STRING
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition offset metadata
%       partition => INT32
%       offset => INT64
%       metadata => NULLABLE_STRING
request_v0(ConsumerGroup, Topics, #{api_version := ApiVersion} = State) ->
  kafe_protocol:request(
    <<
      (kafe_protocol:encode_string(ConsumerGroup))/binary,
      (topics(Topics, ApiVersion))/binary
    >>,
    State).

% OffsetCommit Request (Version: 1) => group_id group_generation_id member_id [topics]
%   group_id => STRING
%   group_generation_id => INT32
%   member_id => STRING
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition offset timestamp metadata
%       partition => INT32
%       offset => INT64
%       timestamp => INT64
%       metadata => NULLABLE_STRING
request_v1(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, Topics, #{api_version := ApiVersion} = State) ->
  kafe_protocol:request(
    <<
      (kafe_protocol:encode_string(ConsumerGroup))/binary,
      ConsumerGroupGenerationId:32/signed,
      (kafe_protocol:encode_string(ConsumerId))/binary,
      (topics(Topics, ApiVersion))/binary
    >>,
    State).

% OffsetCommit Request (Version: 2) => group_id group_generation_id member_id retention_time [topics]
%   group_id => STRING
%   group_generation_id => INT32
%   member_id => STRING
%   retention_time => INT64
%   topics => topic [partitions]
%     topic => STRING
%     partitions => partition offset metadata
%       partition => INT32
%       offset => INT64
%       metadata => NULLABLE_STRING
request_v2(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, Topics, #{api_version := ApiVersion} = State) ->
  kafe_protocol:request(
    <<
      (kafe_protocol:encode_string(ConsumerGroup))/binary,
      ConsumerGroupGenerationId:32/signed,
      (kafe_protocol:encode_string(ConsumerId))/binary,
      RetentionTime:64/signed,
      (topics(Topics, ApiVersion))/binary
    >>,
    State).

% OffsetCommit Response (Version: 0) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code
%       partition => INT32
%       error_code => INT16
%
% OffsetCommit Response (Version: 1) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code
%       partition => INT32
%       error_code => INT16
%
% OffsetCommit Response (Version: 2) => [responses]
%   responses => topic [partition_responses]
%     topic => STRING
%     partition_responses => partition error_code
%       partition => INT32
%       error_code => INT16
response(<<NumberOfTopics:32/signed, Remainder/binary>>, _State) ->
  {ok, response2(NumberOfTopics, Remainder)}.

% Private

topics(Topics, ApiVersion) ->
  topics(Topics, ApiVersion, []).

topics([], _, Acc) ->
  kafe_protocol:encode_array(lists:reverse(Acc));

topics([{TopicName, Partitions}|Rest], ApiVersion, Acc) when ApiVersion == ?V0;
                                                             ApiVersion == ?V2 ->
  topics(Rest,
         ApiVersion,
         [<<
            (kafe_protocol:encode_string(TopicName))/binary,
            (kafe_protocol:encode_array(
               [begin
                  {Partition, Offset, Metadata} = check_partition(P, ApiVersion),
                  <<Partition:32/signed, Offset:64/signed, (kafe_protocol:encode_string(Metadata))/binary>>
                end || P <- Partitions]
              ))/binary
          >> | Acc]);
topics([{TopicName, Partitions}|Rest], ApiVersion, Acc) when ApiVersion == ?V1 ->
  topics(Rest,
         ApiVersion,
         [<<
            (kafe_protocol:encode_string(TopicName))/binary,
            (kafe_protocol:encode_array(
               [begin
                  {Partition, Offset, Timestamp, Metadata} = check_partition(P, ApiVersion),
                  <<Partition:32/signed,
                    Offset:64/signed,
                    Timestamp:64/signed,
                    (kafe_protocol:encode_string(Metadata))/binary>>
                end || P <- Partitions]
              ))/binary
          >> | Acc]).

check_partition({Partition, Offset}, ApiVersion) when (ApiVersion == ?V0 orelse
                                                       ApiVersion == ?V2),
                                                      is_integer(Partition),
                                                      is_integer(Offset) ->
  {Partition, Offset, undefined};
check_partition({Partition, Offset}, ApiVersion) when ApiVersion == ?V1,
                                                      is_integer(Partition),
                                                      is_integer(Offset) ->
  {Partition, Offset, kafe_utils:timestamp(), undefined};
check_partition({Partition, Offset, Timestamp}, ApiVersion) when (ApiVersion == ?V0 orelse
                                                                  ApiVersion == ?V2),
                                                                 is_integer(Partition),
                                                                 is_integer(Offset),
                                                                 is_integer(Timestamp) ->
  {Partition, Offset, undefined};
check_partition({Partition, Offset, Timestamp}, ApiVersion) when ApiVersion == ?V1,
                                                                 is_integer(Partition),
                                                                 is_integer(Offset),
                                                                 is_integer(Timestamp) ->
  {Partition, Offset, Timestamp, undefined};
check_partition({Partition, Offset, Metadata}, ApiVersion) when (ApiVersion == ?V0 orelse
                                                                 ApiVersion == ?V2),
                                                                is_integer(Partition),
                                                                is_integer(Offset),
                                                                is_binary(Metadata) ->
  {Partition, Offset, Metadata};
check_partition({Partition, Offset, Metadata}, ApiVersion) when ApiVersion == ?V1,
                                                                is_integer(Partition),
                                                                is_integer(Offset),
                                                                is_binary(Metadata) ->
  {Partition, Offset, kafe_utils:timestamp(), Metadata};
check_partition({Partition, Offset, Timestamp, Metadata}, ApiVersion) when (ApiVersion == ?V0 orelse
                                                                            ApiVersion == ?V2),
                                                                           is_integer(Partition),
                                                                           is_integer(Offset),
                                                                           is_integer(Timestamp),
                                                                           is_binary(Metadata) ->
  {Partition, Offset, Metadata};
check_partition({Partition, Offset, Timestamp, Metadata}, ApiVersion) when ApiVersion == ?V1,
                                                                           is_integer(Partition),
                                                                           is_integer(Offset),
                                                                           is_integer(Timestamp),
                                                                           is_binary(Metadata) ->
  {Partition, Offset, Timestamp, Metadata};
check_partition(P, _) ->
  P.

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

-ifdef(TEST).
kafe_protocol_consumer_offset_commit_test_() ->
  {setup,
   fun() ->
       meck:new(kafe_utils, [passthrough]),
       meck:expect(kafe_utils, timestamp, 0, 123456789),
       ok
   end,
   fun(_) ->
       meck:unload(kafe_utils),
       ok
   end,
   [
    fun() ->
        ?assertEqual(
           {1, 1000, 999999999, <<"metadata">>},
           check_partition({1, 1000, 999999999, <<"metadata">>}, ?V1)
          ),
        ?assertEqual(
           {1, 1000, <<"metadata">>},
           check_partition({1, 1000, 999999999, <<"metadata">>}, ?V0)
          ),
        ?assertEqual(
           {1, 1000, <<"metadata">>},
           check_partition({1, 1000, 999999999, <<"metadata">>}, ?V2)
          ),

        ?assertEqual(
           {1, 1000, 999999999, undefined},
           check_partition({1, 1000, 999999999}, ?V1)
          ),
        ?assertEqual(
           {1, 1000, undefined},
           check_partition({1, 1000, 999999999}, ?V0)
          ),
        ?assertEqual(
           {1, 1000, undefined},
           check_partition({1, 1000, 999999999}, ?V2)
          ),

        ?assertEqual(
           {1, 1000, 123456789, <<"metadata">>},
           check_partition({1, 1000, <<"metadata">>}, ?V1)
          ),
        ?assertEqual(
           {1, 1000, <<"metadata">>},
           check_partition({1, 1000, <<"metadata">>}, ?V0)
          ),
        ?assertEqual(
           {1, 1000, <<"metadata">>},
           check_partition({1, 1000, <<"metadata">>}, ?V2)
          ),

        ?assertEqual(
           {1, 1000, 123456789, undefined},
           check_partition({1, 1000}, ?V1)
          ),
        ?assertEqual(
           {1, 1000, undefined},
           check_partition({1, 1000}, ?V0)
          ),
        ?assertEqual(
           {1, 1000, undefined},
           check_partition({1, 1000}, ?V2)
          )
    end
   ]}.
-endif.

