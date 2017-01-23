% @hidden
-module(kafe_protocol_group_coordinator).

-include("../include/kafe.hrl").

-export([
         run/2,
         run/1,
         request/2,
         response/2
        ]).

run(ConsumerGroup, force) ->
  case kafe_protocol:run({call,
                          fun ?MODULE:request/2, [ConsumerGroup],
                          fun ?MODULE:response/2}) of
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

request(ConsumerGroup, State) ->
  kafe_protocol:request(
    ?GROUP_COORDINATOR_REQUEST,
    <<(kafe_protocol:encode_string(ConsumerGroup))/binary>>,
    State,
    ?V0).

response(<<ErrorCode:16/signed,
           CoordinatorID:32/signed,
           CoordinatorHostLength:16/signed,
           CoordinatorHost:CoordinatorHostLength/bytes,
           CoordinatorPort:32/signed>>, _ApiVersion) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         coordinator_id => CoordinatorID,
         coordinator_host => CoordinatorHost,
         coordinator_port => CoordinatorPort}}.

