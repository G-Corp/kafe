% @hidden
-module(kafe_protocol_heartbeat).

-include("../include/kafe.hrl").

-export([
         run/3,
         request/4,
         response/2
        ]).

run(GroupId, GenerationId, MemberId) ->
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
                       fun ?MODULE:request/4, [GroupId, GenerationId, MemberId],
                       fun ?MODULE:response/2},
                       infinity)
  end.

% Heartbeat Request (Version: 0) => group_id group_generation_id member_id
%   group_id => STRING
%   group_generation_id => INT32
%   member_id => STRING
request(GroupId, GenerationId, MemberId, State) ->
  kafe_protocol:request(
    ?HEARTBEAT_REQUEST,
    <<(kafe_protocol:encode_string(GroupId))/binary,
      GenerationId:32/signed,
      (kafe_protocol:encode_string(MemberId))/binary>>,
    State,
    ?V0).

% Heartbeat Response (Version: 0) => error_code
%   error_code => INT16
response(<<ErrorCode:16/signed>>,
         _ApiVersion) ->
      {ok, #{error_code => kafe_error:code(ErrorCode)}}.

