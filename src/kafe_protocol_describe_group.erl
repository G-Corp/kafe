% @hidden
-module(kafe_protocol_describe_group).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 0).

-export([
         run/1,
         request/2,
         response/2
        ]).

run(GroupId) ->
  kafe_protocol:run(
    ?DESCRIBE_GROUPS_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request/2, [GroupId]},
    fun ?MODULE:response/2,
    #{broker => {coordinator, GroupId}}).

% DescribeGroups Request (Version: 0) => [group_ids]
request(GroupId, State) ->
  kafe_protocol:request(
    <<(kafe_protocol:encode_array([kafe_protocol:encode_string(GroupId)]))/binary>>,
    State).

% DescribeGroups Response (Version: 0) => [groups]
%   groups => error_code group_id state protocol_type protocol [members]
%   error_code => INT16
%   group_id => STRING
%   state => STRING
%   protocol_type => STRING
%   protocol => STRING
%   members => member_id client_id client_host member_metadata member_assignment
%     member_id => STRING
%     client_id => STRING
%     client_host => STRING
%     member_metadata => BYTES % Q: user_data ?
%     member_assignment => BYTES
%       MemberAssignment => Version PartitionAssignment
%         Version => int16
%         PartitionAssignment => [Topic [Partition]]
%           Topic => string
%           Partition => int32
%         UserData => bytes ??? NOT ???
response(<<GroupesLength:32/signed,
           Remainder/binary>>,
         _State) ->
  {ok, groups(GroupesLength, Remainder, [])}.

groups(0, _, Acc) ->
  Acc;
groups(N, <<ErrorCode:16/signed,
            GroupIdSize:16/signed,
            GroupId:GroupIdSize/binary,
            StateSize:16/signed,
            State:StateSize/binary,
            ProtocolTypeSize:16/signed,
            ProtocolType:ProtocolTypeSize/binary,
            ProtocolSize:16/signed,
            Protocol:ProtocolSize/binary,
            MembersSize:32/signed,
            Remainder/binary>>, Acc) ->
  {Remainder1, Members} = members(MembersSize, Remainder, []),
  groups(N - 1, Remainder1, [#{error_code => kafe_error:code(ErrorCode),
                               group_id => GroupId,
                               state => State,
                               protocol_type => ProtocolType,
                               protocol => Protocol,
                               members => Members}|Acc]).

members(0, Remainder, Acc) ->
  {Remainder, Acc};
members(N, <<MemberIdSize:16/signed,
             MemberId:MemberIdSize/binary,
             ClientIdSize:16/signed,
             ClientId:ClientIdSize/binary,
             ClientHostSize:16/signed,
             ClientHost:ClientHostSize/binary,
             MemberMetadataSize:32/signed,
             MemberMetadata:MemberMetadataSize/binary,
             MemberAssignmentSize:32/signed,
             MemberAssignment:MemberAssignmentSize/binary,
             Remainder/binary>>, Acc) ->
  members(N - 1, Remainder, [#{member_id => MemberId,
                               client_id => ClientId,
                               client_host => ClientHost,
                               member_metadata => MemberMetadata,
                               member_assignment => member_assignment(MemberAssignment)}|Acc]).

member_assignment(<<Version:16/signed,
                    PartitionAssignmentSize:32/signed,
                    Remainder/binary>>) ->
  {PartitionAssignment, UserData} = partition_assignment(PartitionAssignmentSize, Remainder, []),
  #{version => Version,
    partition_assignment => PartitionAssignment,
    user_data => UserData};
member_assignment(<<>>) ->
  #{version => -1,
    partition_assignment => [],
    user_data => <<>>}.

partition_assignment(0, <<UserDataSize:32/signed,
                          UserData:UserDataSize/binary>>, Acc) ->
  {Acc, UserData};
partition_assignment(0, _, Acc) ->
  {Acc, <<>>};
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

