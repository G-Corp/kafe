% @hidden
-module(kafe_protocol_sync_group).

-include("../include/kafe.hrl").

-export([
         run/4,
         request/5,
         response/2
        ]).

run(GroupId, GenerationId, MemberId, Assignments) ->
  kafe_protocol:run({coordinator, GroupId},
                    {call,
                     fun ?MODULE:request/5, [GroupId, GenerationId, MemberId, Assignments],
                     fun ?MODULE:response/2}).

% SyncGroup Request (Version: 0) => group_id generation_id member_id [group_assignment]
%   group_id => STRING
%   generation_id => INT32
%   member_id => STRING
%   group_assignment => member_id member_assignment
%     member_id => STRING
%     member_assignment => BYTES
%
% MemberAssignment => Version PartitionAssignment
%   Version => int16
%   PartitionAssignment => [Topic [Partition]] UserData
%     Topic => string
%     Partition => int32
%   UserData => bytes
request(GroupId, GenerationId, MemberId, Assignments, State) ->
  kafe_protocol:request(
    ?SYNC_GROUP_REQUEST,
    <<(kafe_protocol:encode_string(GroupId))/binary,
      GenerationId:32/signed,
      (kafe_protocol:encode_string(MemberId))/binary,
      (group_assignment(Assignments, []))/binary>>,
    State,
    ?V0).

group_assignment([], Acc) ->
  kafe_protocol:encode_array(lists:reverse(Acc));
group_assignment([#{member_id := MemberId,
                    member_assignment := MemberAssignment}|Rest], Acc) ->
  Version = maps:get(version, MemberAssignment, ?DEFAULT_GROUP_PROTOCOL_VERSION),
  Partitions = maps:get(partition_assignment, MemberAssignment, ?DEFAULT_GROUP_PARTITION_ASSIGNMENT),
  UserData = maps:get(user_data, MemberAssignment, ?DEFAULT_GROUP_USER_DATA),
  group_assignment(Rest, [<<(kafe_protocol:encode_string(MemberId))/binary,
                            (kafe_protocol:encode_bytes(
                               <<Version:16/signed,
                                 (partition_assignment(Partitions, []))/binary,
                                 (kafe_protocol:encode_bytes(UserData))/binary>>))/binary>>|Acc]).

partition_assignment([], Acc) ->
  kafe_protocol:encode_array(lists:reverse(Acc));
partition_assignment([#{topic := Topic,
                        partitions := Partitions}|Rest], Acc) ->
  Partitions1 = kafe_protocol:encode_array(lists:map(fun(E) -> <<E:32/signed>> end, Partitions)),
  partition_assignment(Rest, [<<(kafe_protocol:encode_string(Topic))/binary,
                                Partitions1/binary>>|Acc]).

% SyncGroupResponse => ErrorCode MemberAssignment
%   ErrorCode => int16
%   MemberAssignment => bytes
response(<<ErrorCode:16/signed,
           MemberAssignmentSize:32/signed,
           MemberAssignment:MemberAssignmentSize/binary,
           _/binary>>,
         _ApiVersion) ->
  case MemberAssignment of
    <<Version:16/signed,
      PartitionAssignmentSize:32/signed,
      Remainder/binary>> ->
      {PartitionAssignment, UserData} = partition_assignment(PartitionAssignmentSize, Remainder, []),
      {ok, #{error_code => kafe_error:code(ErrorCode),
             version => Version,
             partition_assignment => PartitionAssignment,
             user_data => UserData}};
    _ ->
      {ok, #{error_code => kafe_error:code(ErrorCode),
             version => -1,
             partition_assignment => [],
             user_data => <<>>}}
  end.

partition_assignment(0, <<UserDataSize:32/signed,
                          UserData:UserDataSize/binary>>, Acc) ->
  {Acc, UserData};
partition_assignment(N, <<TopicSize:16/signed,
                          Topic:TopicSize/binary,
                          NbPartitions:32/signed,
                          Remainder/binary>>, Acc) ->
  {Partitions, Remainder1} = partitions(NbPartitions, Remainder, []),
  partition_assignment(N - 1, Remainder1, [#{topic => Topic,
                                             partitions => Partitions}|Acc]).

partitions(0, Remainder, Acc) ->
  {Acc, Remainder};
partitions(N, <<Partition:32/signed,
                Remainder/binary>>, Acc) ->
  partitions(N - 1, Remainder, [Partition|Acc]).

