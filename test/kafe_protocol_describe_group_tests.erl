-module(kafe_protocol_describe_group_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_describe_group_tests_test_() ->
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
             packet => <<0, 15, 0, 0, 0, 0, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 1, 0, 5, 103, 114, 111, 117, 112>>,
             state => #{api_key => ?DESCRIBE_GROUPS_REQUEST,
                        api_version => 0,
                        client_id => <<"test">>,
                        correlation_id => 1}},
           kafe_protocol_describe_group:request(<<"group">>,
                                                #{api_key => ?DESCRIBE_GROUPS_REQUEST,
                                                  api_version => 0,
                                                  client_id => <<"test">>,
                                                  correlation_id => 0}))
    end,
    fun() ->
        ?assertEqual(
           {ok, [#{error_code => none,
                   group_id => <<"mygr">>,
                   members => [#{client_host => <<"/172.18.0.1">>,
                                 client_id => <<"kafe">>,
                                 member_assignment => #{partition_assignment => [],
                                                        user_data => <<>>,
                                                        version => -1},
                                 member_id => <<"kafe-6252f94c-0870-45e5-b627-4ced30198e4f">>,
                                 member_metadata => <<>>}],
                   protocol => <<>>,
                   protocol_type => <<"consumer">>,
                   state => <<"AwaitingSync">>}]},
           kafe_protocol_describe_group:response(
             <<0, 0, 0, 1, 0, 0, 0, 4, 109, 121, 103, 114, 0, 12, 65, 119, 97, 105, 116, 105,
               110, 103, 83, 121, 110, 99, 0, 8, 99, 111, 110, 115, 117, 109, 101, 114, 0, 0,
               0, 0, 0, 1, 0, 41, 107, 97, 102, 101, 45, 54, 50, 53, 50, 102, 57, 52, 99, 45,
               48, 56, 55, 48, 45, 52, 53, 101, 53, 45, 98, 54, 50, 55, 45, 52, 99, 101, 100,
               51, 48, 49, 57, 56, 101, 52, 102, 0, 4, 107, 97, 102, 101, 0, 11, 47, 49, 55,
               50, 46, 49, 56, 46, 48, 46, 49, 0, 0, 0, 0, 0, 0, 0, 0>>,
             x, % TODO delete
             #{api_version => 1}))
    end
   ]}.
