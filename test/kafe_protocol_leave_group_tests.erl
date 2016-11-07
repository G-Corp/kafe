-module(kafe_protocol_leave_group_tests).
-include_lib("eunit/include/eunit.hrl").
-include("kafe_tests.hrl").

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
           #{api_version => 0,
             packet => <<0, 13, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 5, 103, 114, 111, 117, 112, 0, 6, 109,
                         101, 109, 98, 101, 114>>,
             state => ?REQ_STATE2(1, 0)},
           kafe_protocol_leave_group:request(<<"group">>, <<"member">>, ?REQ_STATE2(0, 0))),
        ?assertEqual(
           #{api_version => 0,
             packet => <<0, 13, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 5, 103, 114, 111, 117, 112, 0, 6, 109,
                         101, 109, 98, 101, 114>>,
             state => ?REQ_STATE2(1, 1)},
           kafe_protocol_leave_group:request(<<"group">>, <<"member">>, ?REQ_STATE2(0, 1))),
        ?assertEqual(
           #{api_version => 0,
             packet => <<0, 13, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 5, 103, 114, 111, 117, 112, 0, 6, 109,
                         101, 109, 98, 101, 114>>,
             state => ?REQ_STATE2(1, 2)},
           kafe_protocol_leave_group:request(<<"group">>, <<"member">>, ?REQ_STATE2(0, 2)))
    end,
    fun() ->
        ?assertEqual(
           {ok, #{error_code => none}},
           kafe_protocol_leave_group:response(<<0, 0>>, 0))
    end
   ]}.
