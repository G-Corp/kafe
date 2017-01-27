% @hidden
-module(kafe_protocol_group_coordinator).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/2,
         run/1,
         request/2,
         response/2
        ]).

run(ConsumerGroup, force) ->
  case kafe_protocol:run(
         ?GROUP_COORDINATOR_REQUEST,
         {fun ?MODULE:request/2, [ConsumerGroup]},
         fun ?MODULE:response/2) of
    {ok, #{error_code := none} = Coordinator} = Result ->
      kafe_consumer_store:insert(ConsumerGroup, coordinator, Coordinator),
      Result;
    Other ->
      kafe_consumer_store:delete(ConsumerGroup, coordinator),
      Other
  end.

run(ConsumerGroup) ->
  case kafe_consumer:coordinator(ConsumerGroup) of
    undefined ->
      run(ConsumerGroup, force);
    Data ->
      {ok, Data}
  end.

% GroupCoordinator Request (Version: 0) => group_id
%   group_id => STRING
request(ConsumerGroup, State) ->
  kafe_protocol:request(
    <<(kafe_protocol:encode_string(ConsumerGroup))/binary>>,
    State).

% GroupCoordinator Response (Version: 0) => error_code coordinator
%   error_code => INT16
%   coordinator => node_id host port
%     node_id => INT32
%     host => STRING
%     port => INT32
response(<<ErrorCode:16/signed,
           CoordinatorID:32/signed,
           CoordinatorHostLength:16/signed,
           CoordinatorHost:CoordinatorHostLength/bytes,
           CoordinatorPort:32/signed>>,
         _State) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         coordinator_id => CoordinatorID,
         coordinator_host => CoordinatorHost,
         coordinator_port => CoordinatorPort}}.

