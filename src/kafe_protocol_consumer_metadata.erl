% @hidden
-module(kafe_protocol_consumer_metadata).

-include("../include/kafe.hrl").

-export([
         run/1,
         request/2,
         response/1
        ]).

run(ConsumerGroup) ->
  gen_server:call(kafe:first_broker(),
                  {call, 
                   fun ?MODULE:request/2, [ConsumerGroup],
                   fun ?MODULE:response/1},
                  infinity).

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

