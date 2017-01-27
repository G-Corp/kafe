-module(kafe_protocol_list_groups_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_list_groups_test_() ->
 [
  ?_test(t_request()),
  ?_test(t_response())
 ].

t_request() ->
  ?assertEqual(
     #{api_version => 0,
       packet => <<16:16, 0:16, 0:32, 4:16, "test">>,
       state => #{
         api_version => 0,
         api_key => ?LIST_GROUPS_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_list_groups:request(#{
       api_version => 0,
       api_key => ?LIST_GROUPS_REQUEST,
       correlation_id => 0,
       client_id => <<"test">>
      })).

t_response() ->
  ?assertEqual(
     {ok, #{error_code => none,
            groups => [#{group_id => <<"group1">>,
                         protocol_type => <<"consumer">>}]}},
     kafe_protocol_list_groups:response(
       <<  0:16                      % error code
         , 1:32                      % group count
           , 6:16, "group1"          % group id
           , 8:16, "consumer"        % protocol type
         >>,
      x, % TODO delete
      #{api_version => 0})).
