% @hidden
-module(kafe_protocol_list_groups).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 0).

-export([
         run/1,
         request/1,
         response/2
        ]).

run(BrokerIDOrName) ->
  kafe_protocol:run(
    ?LIST_GROUPS_REQUEST,
    ?MAX_VERSION,
    fun ?MODULE:request/1,
    fun ?MODULE:response/2,
    #{broker => BrokerIDOrName}).

request(State) ->
  kafe_protocol:request(<<>>, State).

% ListGroups Response (Version: 0) => error_code [groups]
%   error_code => INT16
%   groups => group_id protocol_type
%     group_id => STRING
%     protocol_type => STRING
response(<<ErrorCode:16/signed, GroupsLength:32/signed, Remainder/binary>>, _State) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         groups => groups(GroupsLength, Remainder, [])}}.

groups(0, _, Groups) ->
  Groups;
groups(N, <<GroupIDLength:16/signed,
              GroupID:GroupIDLength/binary,
              ProtocolTypeLength:16/signed,
              ProtocolType:ProtocolTypeLength/binary,
              Remainder/binary>>, Acc) ->
  groups(N - 1, Remainder, [#{group_id => GroupID, protocol_type => ProtocolType}|Acc]).

