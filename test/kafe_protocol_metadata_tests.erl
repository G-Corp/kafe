-module(kafe_protocol_metadata_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_metadata_test_() ->
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
  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16  % API key
                    , 0:16                % API version
                    , 0:32                % correlation ID
                    , 4:16, "test"        % client ID
                    , 0:32                % topics count
                 >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 0,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{}, #{api_key => ?METADATA_REQUEST,
                                               api_version => 0,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16  % API key
                    , 1:16                % API version
                    , 0:32                % correlation ID
                    , 4:16, "test"        % client ID
                    , -1:32               % topics count
                 >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 1,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{}, #{api_version => 1,
                                               api_key => ?METADATA_REQUEST,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),
  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16  % API key
                    , 2:16                % API version
                    , 0:32                % correlation ID
                    , 4:16, "test"        % client ID
                    , -1:32               % topics count
                 >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 2,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{}, #{api_version => 2,
                                               api_key => ?METADATA_REQUEST,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16  % API key
                    , 3:16                % API version
                    , 0:32                % correlation ID
                    , 4:16, "test"        % client ID
                    , -1:32               % topics count
                 >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 3,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{}, #{api_version => 3,
                                               api_key => ?METADATA_REQUEST,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16  % API key
                    , 4:16                % API version
                    , 0:32                % correlation ID
                    , 4:16, "test"        % client ID
                    , -1:32               % topics count
                    , 0
                 >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 4,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{}, #{api_version => 4,
                                               api_key => ?METADATA_REQUEST,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16  % API key
                    , 5:16                % API version
                    , 0:32                % correlation ID
                    , 4:16, "test"        % client ID
                    , -1:32               % topics count
                    , 0
                 >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 5,
                  correlation_id => 1,
                  client_id => <<"test">>}},
     kafe_protocol_metadata:request([], #{}, #{api_version => 5,
                                               api_key => ?METADATA_REQUEST,
                                               correlation_id => 0,
                                               client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16   % API key
                    , 0:16                 % API version
                    , 0:32                 % correlation ID
                    , 4:16, "test"         % client ID
                    , 2:32                 % topics count
                      , 6:16, "topic1"
                      , 6:16, "topic2"
                  >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{},
                                    #{api_version => 0,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16   % API key
                    , 1:16                 % API version
                    , 0:32                 % correlation ID
                    , 4:16, "test"         % client ID
                    , 2:32                 % topics count
                      , 6:16, "topic1"
                      , 6:16, "topic2"
                  >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{},
                                    #{api_version => 1,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16   % API key
                    , 2:16                 % API version
                    , 0:32                 % correlation ID
                    , 4:16, "test"         % client ID
                    , 2:32                 % topics count
                      , 6:16, "topic1"
                      , 6:16, "topic2"
                  >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{},
                                    #{api_version => 2,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16   % API key
                    , 3:16                 % API version
                    , 0:32                 % correlation ID
                    , 4:16, "test"         % client ID
                    , 2:32                 % topics count
                      , 6:16, "topic1"
                      , 6:16, "topic2"
                  >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 3,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{},
                                    #{api_version => 3,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16   % API key
                    , 4:16                 % API version
                    , 0:32                 % correlation ID
                    , 4:16, "test"         % client ID
                    , 2:32                 % topics count
                      , 6:16, "topic1"
                      , 6:16, "topic2"
                    , 0                    % Allow auto topic creation
                  >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 4,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{},
                                    #{api_version => 4,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?METADATA_REQUEST:16   % API key
                    , 5:16                 % API version
                    , 0:32                 % correlation ID
                    , 4:16, "test"         % client ID
                    , 2:32                 % topics count
                      , 6:16, "topic1"
                      , 6:16, "topic2"
                    , 1                    % Allow auto topic creation
                  >>,
       state => #{api_key => ?METADATA_REQUEST,
                  api_version => 5,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_metadata:request([<<"topic1">>, <<"topic2">>],
                                    #{allow_auto_topic_creation => true},
                                    #{api_version => 5,
                                      api_key => ?METADATA_REQUEST,
                                      correlation_id => 0,
                                      client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, #{brokers => [#{host => <<"172.17.0.1">>,
                          id => 3,
                          port => 9094},
                        #{host => <<"172.17.0.1">>,
                          id => 1,
                          port => 9092},
                        #{host => <<"172.17.0.1">>,
                          id => 2,
                          port => 9093}],
            topics => [#{error_code => none,
                         name => <<"topic">>,
                         partitions => [#{error_code => none,
                                          id => 0,
                                          isr => [3, 2, 1],
                                          leader => 1,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 1,
                                          isr => [1, 3, 2],
                                          leader => 2,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 2,
                                          isr => [2, 1, 3],
                                          leader => 3,
                                          replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<3:32                         % brokers count
           % id,   host,                port
           , 2:32, 10:16, "172.17.0.1", 9093:32
           , 1:32, 10:16, "172.17.0.1", 9092:32
           , 3:32, 10:16, "172.17.0.1", 9094:32
         , 1:32                       % topics count
           , 0:16                     % topic error
           , 5:16, "topic"            % topic name
           , 3:32                     % partition count
             , 0:16                   % partition error
             , 0:32                   % partition ID
             , 1:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 3:32, 2:32, 1:32 % isr
             , 0:16                   % partition error
             , 1:32                   % partition ID
             , 2:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 1:32, 3:32, 2:32 % isr
             , 0:16                   % partition error
             , 2:32                   % partition ID
             , 3:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 2:32, 1:32, 3:32 % isr
       >>,
       #{api_version => 0})),

  ?assertEqual(
     {ok, #{brokers => [#{host => <<"172.17.0.1">>,
                          id => 3,
                          port => 9094,
                          rack => <<>>},
                        #{host => <<"172.17.0.1">>,
                          id => 1,
                          port => 9092,
                          rack => <<>>},
                        #{host => <<"172.17.0.1">>,
                          id => 2,
                          port => 9093,
                          rack => <<>>}],
            controller_id => 1,
            topics => [#{error_code => none,
                         is_internal => false,
                         name => <<"topic">>,
                         partitions => [#{error_code => none,
                                          id => 0,
                                          isr => [3, 2, 1],
                                          leader => 1,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 1,
                                          isr => [1, 3, 2],
                                          leader => 2,
                                          replicas => [3, 2, 1]},
                                        #{error_code => none,
                                          id => 2,
                                          isr => [2, 1, 3],
                                          leader => 3,
                                          replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<3:32                         % brokers count
           % id,   host,                port
           , 2:32, 10:16, "172.17.0.1", 9093:32, -1:16
           , 1:32, 10:16, "172.17.0.1", 9092:32, -1:16
           , 3:32, 10:16, "172.17.0.1", 9094:32, -1:16
         , 1:32                       % controller ID
         , 1:32                       % topics count
           , 0:16                     % topic error
           , 5:16, "topic"            % topic name
           , 0                        % is internal
           , 3:32                     % partition count
             , 0:16                   % partition error
             , 0:32                   % partition ID
             , 1:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 3:32, 2:32, 1:32 % isr
             , 0:16                   % partition error
             , 1:32                   % partition ID
             , 2:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 1:32, 3:32, 2:32 % isr
             , 0:16                   % partition error
             , 2:32                   % partition ID
             , 3:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 2:32, 1:32, 3:32 % isr
       >>,
       #{api_version => 1})),

  ?assertEqual(
     {ok,
      #{brokers => [#{host => <<"172.17.0.1">>,
                      id => 3,
                      port => 9094,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 1,
                      port => 9092,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 2,
                      port => 9093,
                      rack => <<>>}],
        cluster_id => <<"ABCDEF">>,
        controller_id => 1,
        topics => [#{error_code => none,
                     is_internal => false,
                     name => <<"topic">>,
                     partitions => [#{error_code => none,
                                      id => 0,
                                      isr => [3, 2, 1],
                                      leader => 1,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 1,
                                      isr => [1, 3, 2],
                                      leader => 2,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 2,
                                      isr => [2, 1, 3],
                                      leader => 3,
                                      replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<3:32                         % brokers count
           % id,   host,                port
           , 2:32, 10:16, "172.17.0.1", 9093:32, -1:16
           , 1:32, 10:16, "172.17.0.1", 9092:32, -1:16
           , 3:32, 10:16, "172.17.0.1", 9094:32, -1:16
         , 6:16, "ABCDEF"             % cluster ID
         , 1:32                       % controller ID
         , 1:32                       % topics count
           , 0:16                     % topic error
           , 5:16, "topic"            % topic name
           , 0                        % is internal
           , 3:32                     % partition count
             , 0:16                   % partition error
             , 0:32                   % partition ID
             , 1:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 3:32, 2:32, 1:32 % isr
             , 0:16                   % partition error
             , 1:32                   % partition ID
             , 2:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 1:32, 3:32, 2:32 % isr
             , 0:16                   % partition error
             , 2:32                   % partition ID
             , 3:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 2:32, 1:32, 3:32 % isr
       >>,
       #{api_version => 2})),

  ?assertEqual(
     {ok,
      #{throttle_time => 0,
        brokers => [#{host => <<"172.17.0.1">>,
                      id => 3,
                      port => 9094,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 1,
                      port => 9092,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 2,
                      port => 9093,
                      rack => <<>>}],
        cluster_id => <<"ABCDEF">>,
        controller_id => 1,
        topics => [#{error_code => none,
                     is_internal => false,
                     name => <<"topic">>,
                     partitions => [#{error_code => none,
                                      id => 0,
                                      isr => [3, 2, 1],
                                      leader => 1,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 1,
                                      isr => [1, 3, 2],
                                      leader => 2,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 2,
                                      isr => [2, 1, 3],
                                      leader => 3,
                                      replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<0:32                         % throttle time
         , 3:32                       % brokers count
           % id,   host,                port
           , 2:32, 10:16, "172.17.0.1", 9093:32, -1:16
           , 1:32, 10:16, "172.17.0.1", 9092:32, -1:16
           , 3:32, 10:16, "172.17.0.1", 9094:32, -1:16
         , 6:16, "ABCDEF"             % cluster ID
         , 1:32                       % controller ID
         , 1:32                       % topics count
           , 0:16                     % topic error
           , 5:16, "topic"            % topic name
           , 0                        % is internal
           , 3:32                     % partition count
             , 0:16                   % partition error
             , 0:32                   % partition ID
             , 1:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 3:32, 2:32, 1:32 % isr
             , 0:16                   % partition error
             , 1:32                   % partition ID
             , 2:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 1:32, 3:32, 2:32 % isr
             , 0:16                   % partition error
             , 2:32                   % partition ID
             , 3:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 2:32, 1:32, 3:32 % isr
       >>,
       #{api_version => 3})),

  ?assertEqual(
     {ok,
      #{throttle_time => 0,
        brokers => [#{host => <<"172.17.0.1">>,
                      id => 3,
                      port => 9094,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 1,
                      port => 9092,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 2,
                      port => 9093,
                      rack => <<>>}],
        cluster_id => <<"ABCDEF">>,
        controller_id => 1,
        topics => [#{error_code => none,
                     is_internal => false,
                     name => <<"topic">>,
                     partitions => [#{error_code => none,
                                      id => 0,
                                      isr => [3, 2, 1],
                                      leader => 1,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 1,
                                      isr => [1, 3, 2],
                                      leader => 2,
                                      replicas => [3, 2, 1]},
                                    #{error_code => none,
                                      id => 2,
                                      isr => [2, 1, 3],
                                      leader => 3,
                                      replicas => [3, 2, 1]}]}]}},
     kafe_protocol_metadata:response(
       <<0:32                         % throttle time
         , 3:32                       % brokers count
           % id,   host,                port
           , 2:32, 10:16, "172.17.0.1", 9093:32, -1:16
           , 1:32, 10:16, "172.17.0.1", 9092:32, -1:16
           , 3:32, 10:16, "172.17.0.1", 9094:32, -1:16
         , 6:16, "ABCDEF"             % cluster ID
         , 1:32                       % controller ID
         , 1:32                       % topics count
           , 0:16                     % topic error
           , 5:16, "topic"            % topic name
           , 0                        % is internal
           , 3:32                     % partition count
             , 0:16                   % partition error
             , 0:32                   % partition ID
             , 1:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 3:32, 2:32, 1:32 % isr
             , 0:16                   % partition error
             , 1:32                   % partition ID
             , 2:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 1:32, 3:32, 2:32 % isr
             , 0:16                   % partition error
             , 2:32                   % partition ID
             , 3:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 2:32, 1:32, 3:32 % isr
       >>,
       #{api_version => 4})),

  ?assertEqual(
     {ok,
      #{throttle_time => 0,
        brokers => [#{host => <<"172.17.0.1">>,
                      id => 3,
                      port => 9094,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 1,
                      port => 9092,
                      rack => <<>>},
                    #{host => <<"172.17.0.1">>,
                      id => 2,
                      port => 9093,
                      rack => <<>>}],
        cluster_id => <<"ABCDEF">>,
        controller_id => 1,
        topics => [#{error_code => none,
                     is_internal => false,
                     name => <<"topic">>,
                     partitions => [#{error_code => none,
                                      id => 0,
                                      isr => [3, 2, 1],
                                      leader => 1,
                                      replicas => [3, 2, 1],
                                      offline_replicas => [1]},
                                    #{error_code => none,
                                      id => 1,
                                      isr => [1, 3, 2],
                                      leader => 2,
                                      replicas => [3, 2, 1],
                                      offline_replicas => [2]},
                                    #{error_code => none,
                                      id => 2,
                                      isr => [2, 1, 3],
                                      leader => 3,
                                      replicas => [3, 2, 1],
                                      offline_replicas => [3]}]}]}},
     kafe_protocol_metadata:response(
       <<0:32                         % throttle time
         , 3:32                       % brokers count
           % id,   host,                port
           , 2:32, 10:16, "172.17.0.1", 9093:32, -1:16
           , 1:32, 10:16, "172.17.0.1", 9092:32, -1:16
           , 3:32, 10:16, "172.17.0.1", 9094:32, -1:16
         , 6:16, "ABCDEF"             % cluster ID
         , 1:32                       % controller ID
         , 1:32                       % topics count
           , 0:16                     % topic error
           , 5:16, "topic"            % topic name
           , 0                        % is internal
           , 3:32                     % partition count
             , 0:16                   % partition error
             , 0:32                   % partition ID
             , 1:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 3:32, 2:32, 1:32 % isr
             , 1:32, 1:32             % offline replicas
             , 0:16                   % partition error
             , 1:32                   % partition ID
             , 2:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 1:32, 3:32, 2:32 % isr
             , 1:32, 2:32             % offline replicas
             , 0:16                   % partition error
             , 2:32                   % partition ID
             , 3:32                   % leader
             , 3:32, 3:32, 2:32, 1:32 % replicas
             , 3:32, 2:32, 1:32, 3:32 % isr
             , 1:32, 3:32             % offline replicas
       >>,
       #{api_version => 5})).
