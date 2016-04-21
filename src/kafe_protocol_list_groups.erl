% @hidden
-module(kafe_protocol_list_groups).

-include("../include/kafe.hrl").

-export([
         run/1,
         request/1,
         response/2
        ]).

run(BrokerID) ->
  kafe_protocol:run(BrokerID,
                    {call,
                     fun ?MODULE:request/1, [],
                     fun ?MODULE:response/2}).

request(State) ->
  kafe_protocol:request(?LIST_GROUPS_REQUEST, <<>>, State, ?V0).

% ListGroups Response (Version: 0) => error_code [groups]
%   error_code => INT16
%   groups => group_id protocol_type
%     group_id => STRING
%     protocol_type => STRING
response(<<ErrorCode:16/signed, GroupsLength:16/signed, Remainder/binary>>, _ApiVersion) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         groups => response(GroupsLength, Remainder, [])}}.

response(0, _, Groups) ->
  Groups;
response(N, <<GroupIDLength:16/signed,
              GroupID:GroupIDLength/binary,
              ProtocolTypeLength:16/signed,
              ProtocolType:ProtocolTypeLength/binary,
              Remainder/binary>>, Acc) ->
  response(N - 1, Remainder, [#{group_id => GroupID, protocol_type => ProtocolType}|Acc]).

