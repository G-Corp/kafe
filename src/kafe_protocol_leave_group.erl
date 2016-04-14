% @hidden
-module(kafe_protocol_leave_group).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/2
        ]).

run(GroupId, MemberId) ->
  CoordinatorBrocker = case kafe:group_coordinator(bucs:to_binary(GroupId)) of
                         {ok, #{coordinator_host := CoordinatorHost,
                                coordinator_port := CoordinatorPort,
                                error_code := none}} ->
                           kafe:broker_by_host_and_port(CoordinatorHost, CoordinatorPort);
                         _ ->
                           undefined
                       end,
  case CoordinatorBrocker of
    undefined -> {error, no_broker_found};
    Broker ->
      gen_server:call(Broker,
                      {call,
                       fun ?MODULE:request/3, [GroupId, MemberId],
                       fun ?MODULE:response/2},
                       infinity)
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

