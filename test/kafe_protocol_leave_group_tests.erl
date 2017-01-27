-module(kafe_protocol_leave_group_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_leave_group_tests_test_() ->
  {setup,
   fun() ->
       ok
   end,
   fun(_) ->
       ok
   end,
   [
    fun() ->
        ?assertEqual(
           #{packet => <<0, 13, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 5, 103, 114, 111, 117, 112, 0, 6, 109,
                         101, 109, 98, 101, 114>>,
             state => #{
               api_version => 0,
               api_key => ?LEAVE_GROUP_REQUEST,
               correlation_id => 1,
               client_id => <<"test">>
              }},
           kafe_protocol_leave_group:request(<<"group">>, <<"member">>,
                                             #{api_version => 0,
                                               api_key => ?LEAVE_GROUP_REQUEST,
                                               correlation_id => 0,
                                               client_id => <<"test">>}))
    end,
    fun() ->
        ?assertEqual(
           {ok, #{error_code => none}},
           kafe_protocol_leave_group:response(<<0, 0>>,
                                              #{api_version => 0}))
    end
   ]}.
