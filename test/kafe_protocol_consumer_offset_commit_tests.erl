-module(kafe_protocol_consumer_offset_commit_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_consumer_offset_commit_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request())
    , ?_test(t_response())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request() ->
  ?assertEqual(
     #{packet => << ?OFFSET_COMMIT_REQUEST:16    % API key
                    , 0:16                       % API version
                    , 0:32                       % correlation ID
                    , 4:16, "test"               % client ID
                    , 13:16, "ConsumerGroup"     % group ID
                    , 1:32                       % topics count
                      , 5:16, "topic"            % topic name
                      , 3:32                     % partitions count
                        % partition, offset,  metadata
                        , 0:32,      1000:64, -1:16
                        , 1:32,      1001:64, 8:16, "metadata"
                        , 2:32,      1002:64, -1:16
                  >>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v0(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [{0, 1000}, {1, 1001, <<"metadata">>}, {2, 1002}]}],
       #{api_version => 0,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?OFFSET_COMMIT_REQUEST:16    % API key
                    , 1:16                       % API version
                    , 0:32                       % correlation ID
                    , 4:16, "test"               % client ID
                    , 13:16, "ConsumerGroup"     % group ID
                    , 1:32                       % group generation ID
                    , 10:16, "ConsumerId"        % member ID
                    , 1:32                       % topics count
                      , 5:16, "topic"            % Topic name
                      , 3:32                     % partitions count
                        , 0:32, 1000:64, 123456789:64, -1:16
                        , 1:32, 1001:64, 123456789:64, 8:16, "metadata"
                        , 2:32, 1002:64, 123456789:64, -1:16
                  >>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v1(
       <<"ConsumerGroup">>,
       1,
       <<"ConsumerId">>,
       [{<<"topic">>, [{0, 1000, 123456789}, {1, 1001, 123456789, <<"metadata">>}, {2, 1002, 123456789}]}],
       #{api_version => 1,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?OFFSET_COMMIT_REQUEST:16    % API key
                    , 2:16                       % API version
                    , 0:32                       % correlation ID
                    , 4:16, "test"               % client ID
                    , 13:16, "ConsumerGroup"     % group ID
                    , 1:32                       % group generation ID
                    , 10:16, "ConsumerId"        % member ID
                    , 3:64                       % retention time
                    , 1:32                       % topics count
                      , 5:16, "topic"            % Topic name
                      , 3:32                     % partitions count
                        , 0:32, 1000:64, -1:16
                        , 1:32, 1001:64, 8:16, "metadata"
                        , 2:32, 1002:64, -1:16
                 >>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v2(
       <<"ConsumerGroup">>,
       1,
       <<"ConsumerId">>,
       3,
       [{<<"topic">>, [{0, 1000}, {1, 1001, <<"metadata">>}, {2, 1002}]}],
       #{api_version => 2,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?OFFSET_COMMIT_REQUEST:16    % API key
                    , 3:16                       % API version
                    , 0:32                       % correlation ID
                    , 4:16, "test"               % client ID
                    , 13:16, "ConsumerGroup"     % group ID
                    , 1:32                       % group generation ID
                    , 10:16, "ConsumerId"        % member ID
                    , 3:64                       % retention time
                    , 1:32                       % topics count
                      , 5:16, "topic"            % Topic name
                      , 3:32                     % partitions count
                        , 0:32, 1000:64, -1:16
                        , 1:32, 1001:64, 8:16, "metadata"
                        , 2:32, 1002:64, -1:16
                 >>,
       state => #{api_key => ?OFFSET_COMMIT_REQUEST,
                  api_version => 3,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_consumer_offset_commit:request_v2(
       <<"ConsumerGroup">>,
       1,
       <<"ConsumerId">>,
       3,
       [{<<"topic">>, [{0, 1000}, {1, 1001, <<"metadata">>}, {2, 1002}]}],
       #{api_version => 3,
         api_key => ?OFFSET_COMMIT_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<1:32            % topics count
         , 5:16, "topic" % topic name
         , 1:32          % partitions count
           , 0:32        % partition ID
           , 0:16        % error code
       >>,
       #{api_version => 0})),
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<1:32            % topics count
         , 5:16, "topic" % topic name
         , 1:32          % partitions count
           , 0:32        % partition ID
           , 0:16        % error code
       >>,
       #{api_version => 1})),
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions => [#{error_code => none, partition => 0}]}]},
     kafe_protocol_consumer_offset_commit:response(
       <<1:32            % topics count
         , 5:16, "topic" % topic name
         , 1:32          % partitions count
           , 0:32        % partition ID
           , 0:16        % error code
       >>,
       #{api_version => 2})),
  ?assertEqual(
     {ok, #{throttle_time => 123,
            topics => [#{name => <<"topic">>,
                         partitions => [#{error_code => none, partition => 0}]}]}},
     kafe_protocol_consumer_offset_commit:response(
       <<123:32          % throttle time
         , 1:32          % topics count
         , 5:16, "topic" % topic name
         , 1:32          % partitions count
           , 0:32        % partition ID
           , 0:16        % error code
       >>,
       #{api_version => 3})).
