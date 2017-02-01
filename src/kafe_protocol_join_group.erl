% @hidden
-module(kafe_protocol_join_group).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 1).

-export([
         run/2,
         request/3,
         response/2
        ]).

run(GroupId, Options) ->
  kafe_protocol:run(
    ?JOIN_GROUP_REQUEST,
    ?MAX_VERSION,
    {fun ?MODULE:request/3, [GroupId, Options]},
    fun ?MODULE:response/2,
    #{broker => {coordinator, GroupId}}).

% JoinGroup Request (Version: 0) => group_id session_timeout member_id protocol_type [group_protocols]
%   group_id => STRING
%   session_timeout => INT32
%   member_id => STRING
%   protocol_type => STRING
%   group_protocols => protocol_name protocol_metadata
%     protocol_name => STRING
%     protocol_metadata => BYTES
%
% JoinGroup Request (Version: 1) => group_id session_timeout rebalance_timeout member_id protocol_type [group_protocols]
%   group_id => STRING
%   session_timeout => INT32
%   rebalance_timeout => INT32
%   member_id => STRING
%   protocol_type => STRING
%   group_protocols => protocol_name protocol_metadata
%     protocol_name => STRING
%     protocol_metadata => BYTES
request(GroupId, Options, #{api_version := ApiVersion} = State) when ApiVersion == ?V0 ->
  SessionTimeout = maps:get(session_timeout, Options, ?DEFAULT_JOIN_GROUP_SESSION_TIMEOUT),
  MemberId = maps:get(member_id, Options, ?DEFAULT_JOIN_GROUP_MEMBER_ID),
  ProtocolType = maps:get(protocol_type, Options, ?DEFAULT_JOIN_GROUP_PROTOCOL_TYPE),
  GroupProtocols = maps:get(protocols, Options, ?DEFAULT_JOIN_GROUP_PROTOCOLS),
  kafe_protocol:request(
    <<(kafe_protocol:encode_string(GroupId))/binary,
      SessionTimeout:32/signed,
      (kafe_protocol:encode_string(MemberId))/binary,
      (kafe_protocol:encode_string(ProtocolType))/binary,
      (kafe_protocol:encode_array(GroupProtocols))/binary>>,
    State);
request(GroupId, Options, #{api_version := ApiVersion} = State) when ApiVersion == ?V1 ->
  SessionTimeout = maps:get(session_timeout, Options, ?DEFAULT_JOIN_GROUP_SESSION_TIMEOUT),
  RebalanceTimeout = maps:get(rebalance_timeout, Options, ?DEFAULT_JOIN_GROUP_REBALANCE_TIMEOUT),
  MemberId = maps:get(member_id, Options, ?DEFAULT_JOIN_GROUP_MEMBER_ID),
  ProtocolType = maps:get(protocol_type, Options, ?DEFAULT_JOIN_GROUP_PROTOCOL_TYPE),
  GroupProtocols = maps:get(protocols, Options, ?DEFAULT_JOIN_GROUP_PROTOCOLS),
  kafe_protocol:request(
    <<(kafe_protocol:encode_string(GroupId))/binary,
      SessionTimeout:32/signed,
      RebalanceTimeout:32/signed,
      (kafe_protocol:encode_string(MemberId))/binary,
      (kafe_protocol:encode_string(ProtocolType))/binary,
      (kafe_protocol:encode_array(GroupProtocols))/binary>>,
    State).

% JoinGroup Response (Version: 0) => error_code generation_id group_protocol leader_id member_id [members]
%   error_code => INT16
%   generation_id => INT32
%   group_protocol => STRING
%   leader_id => STRING
%   member_id => STRING
%   members => member_id member_metadata
%     member_id => STRING
%     member_metadata => BYTES
%
% JoinGroup Response (Version: 1) => error_code generation_id group_protocol leader_id member_id [members]
%   error_code => INT16
%   generation_id => INT32
%   group_protocol => STRING
%   leader_id => STRING
%   member_id => STRING
%   members => member_id member_metadata
%     member_id => STRING
%     member_metadata => BYTES
response(<<ErrorCode:16/signed,
           GenerationId:32/signed,
           ProtocolGroupSize:16/signed,
           ProtocolGroup:ProtocolGroupSize/binary,
           LeaderIdSize:16/signed,
           LeaderId:LeaderIdSize/binary,
           MemberIdSize:16/signed,
           MemberId:MemberIdSize/binary,
           MembersLength:32/signed,
           Members/binary>>,
         #{api_version := ApiVersion}) when ApiVersion == ?V0;
                                           ApiVersion == ?V1 ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         generation_id => GenerationId,
         protocol_group => ProtocolGroup,
         leader_id => LeaderId,
         member_id => MemberId,
         members => members(MembersLength, Members, [])}}.

members(0, _, Acc) ->
  Acc;
members(N, <<MemberIdSize:16/signed,
             MemberId:MemberIdSize/binary,
             MemberMetadataSize:32/signed,
             MemberMetadata:MemberMetadataSize/binary,
             Rest/binary>>, Acc) ->
  members(N - 1, Rest, [#{member_id => MemberId,
                          member_metadata => MemberMetadata}|Acc]).

