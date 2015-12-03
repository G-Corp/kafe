-module(kafe_protocol_consumer_metadata_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

kafe_protocol_consumer_metadata_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request()),
    ?_test(t_response())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request() ->
  ?assertEqual(kafe_protocol_consumer_metadata:request(<<"hello">>, ?REQ_STATE(0)),
               #{packet => <<0,0,0,21,0,10,0,1,0,0,0,0,0,4,116,101,115,116,0,5,104,101,108,108,111>>,
                 state => ?REQ_STATE(1)}).

t_response() ->
  ?assertEqual(kafe_protocol_consumer_metadata:response(
                 <<0,0,0,0,0,1,0,9,108,111,99,97,108,104,111,115,116,0,0,31,144>>),
               {ok, #{error_code => no_error,
                      coordinator_id => 1,
                      coordinator_host => <<"localhost">>,
                      coordinator_port => 8080}}).

