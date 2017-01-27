-module(kafe_protocol_sync_group_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_sync_group_tests_test_() ->
  {setup,
   fun() ->
       meck:new(kafe, [passthrough]),
       meck:expect(kafe, topics, 0, #{<<"testone">> => #{0 => "172.17.0.1:9094"},
                                      <<"testthree">> => #{0 => "172.17.0.1:9094", 1 => "172.17.0.1:9092", 2 => "172.17.0.1:9093"},
                                      <<"testtwo">> => #{0 => "172.17.0.1:9092", 1 => "172.17.0.1:9093"}}),
       ok
   end,
   fun(_) ->
       meck:unload(kafe),
       ok
   end,
   [
    fun() ->
        ?assertEqual(
           #{api_version => 0,
             packet => <<0, 14, 0, 0, 0, 0, 0, 2, 0, 4, 107, 97, 102, 101, 0, 4, 109, 121, 103, 114,
                         0, 0, 0, 5, 0, 41, 107, 97, 102, 101, 45, 51, 55, 51, 48, 102, 54, 48, 101,
                         45, 100, 102, 102, 102, 45, 52, 48, 98, 52, 45, 97, 54, 100, 97, 45, 99, 56,
                         53, 57, 101, 51, 49, 50, 102, 56, 50, 53, 0, 0, 0, 1, 0, 41, 107, 97, 102,
                         101, 45, 51, 55, 51, 48, 102, 54, 48, 101, 45, 100, 102, 102, 102, 45, 52,
                         48, 98, 52, 45, 97, 54, 100, 97, 45, 99, 56, 53, 57, 101, 51, 49, 50, 102,
                         56, 50, 53, 0, 0, 0, 75, 0, 0, 0, 0, 0, 3, 0, 7, 116, 101, 115, 116, 116,
                         119, 111, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 9, 116, 101, 115, 116, 116,
                         104, 114, 101, 101, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 7,
                         116, 101, 115, 116, 111, 110, 101, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>,
             state => #{api_key => ?SYNC_GROUP_REQUEST,
                        api_version => 0,
                        client_id => <<"kafe">>,
                        correlation_id => 3}},
           kafe_protocol_sync_group:request(
             <<"mygr">>,
             5,
             <<"kafe-3730f60e-dfff-40b4-a6da-c859e312f825">>,
             [#{member_assignment => #{},
                member_id => <<"kafe-3730f60e-dfff-40b4-a6da-c859e312f825">>}],
             #{api_key => ?SYNC_GROUP_REQUEST,
               api_version => 0,
               client_id => <<"kafe">>,
               correlation_id => 2}))
    end,

    fun() ->
        ?assertEqual(
           {ok, #{error_code => none,
                 partition_assignment => [#{partitions => [0], topic => <<"testone">>},
                                          #{partitions => [2, 1, 0], topic => <<"testthree">>},
                                          #{partitions => [1, 0], topic => <<"testtwo">>}],
                 user_data => <<>>,
                 version => 0}},
           kafe_protocol_sync_group:response(
             <<0, 0, 0, 0, 0, 75, 0, 0, 0, 0, 0, 3, 0, 7, 116, 101, 115, 116, 116, 119, 111, 0, 0, 0, 2,
               0, 0, 0, 0, 0, 0, 0, 1, 0, 9, 116, 101, 115, 116, 116, 104, 114, 101, 101, 0, 0, 0, 3, 0,
               0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 7, 116, 101, 115, 116, 111, 110, 101, 0, 0, 0, 1, 0,
               0, 0, 0, 0, 0, 0, 0>>,
             x, % TODO: delete
             #{api_version => 0}))
    end
   ]}.
