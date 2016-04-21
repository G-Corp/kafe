% @hidden
-module(kafe_protocol_leave_group).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/2
        ]).

run(GroupId, MemberId) ->
  case kafe:group_coordinator(bucs:to_binary(GroupId)) of
    {ok, #{coordinator_host := Host,
           coordinator_port := Port,
           error_code := none}} ->
      kafe_protocol:run({host_and_port, Host, Port},
                        {call,
                         fun ?MODULE:request/3, [GroupId, MemberId],
                         fun ?MODULE:response/2});
    _ ->
      {error, no_broker_found}
  end.

% LeaveGroup Request (Version: 0) => group_id member_id
%   group_id => STRING
%   member_id => STRING
request(GroupId, MemberId, State) ->
  kafe_protocol:request(
    ?LEAVE_GROUP_REQUEST,
    <<(kafe_protocol:encode_string(GroupId))/binary,
      (kafe_protocol:encode_string(MemberId))/binary>>,
    State,
    ?V0).

% LeaveGroup Response (Version: 0) => error_code
%   error_code => INT16
response(<<ErrorCode:16/signed>>,
         _ApiVersion) ->
  {ok, #{error_code => kafe_error:code(ErrorCode)}}.

