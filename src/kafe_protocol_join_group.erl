-module(kafe_protocol_join_group).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/2
        ]).

run(GroupId, Options) ->
  case kafe:first_broker() of
    undefined -> {error, no_broker_found};
    Broker ->
      gen_server:call(Broker,
                      {call,
                       fun ?MODULE:request/3, [GroupId, Options],
                       fun ?MODULE:response/2},
                       infinity)
  end.

request(GroupId, Options, State) ->
  SessionTimeout = maps:get(session_timeout, Options, ?DEFAULT_JOIN_GROUP_SESSION_TIMEOUT),
  MemberId = maps:get(member_id, Options, ?DEFAULT_JOIN_GROUP_MEMBER_ID),
  ProtocolType = maps:get(protocol_type, Options, ?DEFAULT_JOIN_GROUP_PROTOCOL_TYPE),
  GroupProtocols = maps:get(protocols, Options, ?DEFAULT_JOIN_GROUP_PROTOCOLS),
  kafe_protocol:request(
    ?JOIN_GROUP_REQUEST,
    <<(kafe_protocol:encode_string(GroupId))/binary,
      SessionTimeout:32/signed,
      (kafe_protocol:encode_string(MemberId))/binary,
      (kafe_protocol:encode_string(ProtocolType))/binary,
      (kafe_protocol:encode_array(GroupProtocols))/binary>>,
    State,
    ?V0).

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
         _ApiVersion) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         generation_id => GenerationId,
         protocol_group => ProtocolGroup,
         leader_id => LeaderId,
         member_id => MemberId,
         members => response(MembersLength, Members, [])}}.

response(0, _, Acc) ->
  Acc;
response(N, <<MemberIdSize:16/signed,
              MemberId:MemberIdSize/binary,
              MemberMetadataSize:32/signed,
              MemberMetadata:MemberMetadataSize/binary,
              Rest/binary>>, Acc) ->
  response(N - 1, Rest, [#{member_id => MemberId,
                           member_metadata => MemberMetadata}|Acc]).

