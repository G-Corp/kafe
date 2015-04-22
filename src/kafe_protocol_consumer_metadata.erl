% @hidden
-module(kafe_protocol_consumer_metadata).

-include("../include/kafe.hrl").

-export([
         request/2,
         response/1
        ]).

request(ConsumerGroup, State) ->
  kafe_protocol:request(
    ?CONSUMER_METADATA_REQUEST, 
    <<(kafe_protocol:encode_string(ConsumerGroup))/binary>>,
    State).

response(<<ErrorCode:16/signed, 
           CoordinatorID:32/signed,
           CoordinatorHostLength:16/signed,
           CoordinatorHost:CoordinatorHostLength/bytes,
           CoordinatorPort:32/signed>>) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         coordinator_id => CoordinatorID,
         coordinator_host => CoordinatorHost,
         coordinator_port => CoordinatorPort}}.

