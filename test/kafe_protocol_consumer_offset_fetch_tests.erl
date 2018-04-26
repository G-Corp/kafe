-module(kafe_protocol_consumer_offset_fetch_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_consumer_offset_fetch_test_() ->
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
     #{packet => << ?OFFSET_FETCH_REQUEST:16  % API key
                    , 0:16                    % API version
                    , 0:32                    % correlation ID
                    , 4:16, "test"            % client ID
                    , 13:16, "ConsumerGroup"  % group ID
                    , 1:32                    % topics count
                      , 5:16, "topic"         % topic name
                      , 3:32                  % partitions count
                        , 0:32, 1:32, 2:32    % partitions
                 >>,
       state => #{
         api_version => 0,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_consumer_offset_fetch:request(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [0, 1, 2]}],
       #{api_version => 0,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?OFFSET_FETCH_REQUEST:16  % API key
                    , 1:16                    % API version
                    , 0:32                    % correlation ID
                    , 4:16, "test"            % client ID
                    , 13:16, "ConsumerGroup"  % group ID
                    , 1:32                    % topics count
                      , 5:16, "topic"         % topic name
                      , 3:32                  % partitions count
                        , 0:32, 1:32, 2:32    % partitions
                 >>,
       state => #{
         api_version => 1,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_consumer_offset_fetch:request(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [0, 1, 2]}],
       #{api_version => 1,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?OFFSET_FETCH_REQUEST:16  % API key
                    , 2:16                    % API version
                    , 0:32                    % correlation ID
                    , 4:16, "test"            % client ID
                    , 13:16, "ConsumerGroup"  % group ID
                    , 1:32                    % topics count
                      , 5:16, "topic"         % topic name
                      , 3:32                  % partitions count
                        , 0:32, 1:32, 2:32    % partitions
                 >>,
       state => #{
         api_version => 2,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_consumer_offset_fetch:request(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [0, 1, 2]}],
       #{api_version => 2,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?OFFSET_FETCH_REQUEST:16  % API key
                    , 3:16                    % API version
                    , 0:32                    % correlation ID
                    , 4:16, "test"            % client ID
                    , 13:16, "ConsumerGroup"  % group ID
                    , 1:32                    % topics count
                      , 5:16, "topic"         % topic name
                      , 3:32                  % partitions count
                        , 0:32, 1:32, 2:32    % partitions
                 >>,
       state => #{
         api_version => 3,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 1,
         client_id => <<"test">>
        }},
     kafe_protocol_consumer_offset_fetch:request(
       <<"ConsumerGroup">>,
       [{<<"topic">>, [0, 1, 2]}],
       #{api_version => 3,
         api_key => ?OFFSET_FETCH_REQUEST,
         correlation_id => 0,
         client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions_offset => [#{error_code => none,
                                    metadata => <<>>,
                                    offset => 1000,
                                    partition => 0},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<"metadata">>,
                                    offset => -1,
                                    partition => 1},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 2}]}]},
     kafe_protocol_consumer_offset_fetch:response(
       << 1:32              % topics count
            , 5:16, "topic" % topic name
            , 3:32          % partitions count
              % partition, offset,  metadata,         error code
              , 0:32,      1000:64, -1:16,            0:16
              , 1:32,      -1:64,   8:16, "metadata", 3:16
              , 2:32,      -1:64,   -1:16,            3:16
       >>,
       #{api_version => 0})),

  ?assertEqual(
     {ok, [#{name => <<"topic">>,
            partitions_offset => [#{error_code => none,
                                    metadata => <<>>,
                                    offset => 1000,
                                    partition => 0},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<"metadata">>,
                                    offset => -1,
                                    partition => 1},
                                  #{error_code => unknown_topic_or_partition,
                                    metadata => <<>>,
                                    offset => -1,
                                    partition => 2}]}]},
     kafe_protocol_consumer_offset_fetch:response(
       << 1:32              % topics count
            , 5:16, "topic" % topic name
            , 3:32          % partitions count
              % partition, offset,  metadata,         error code
              , 0:32,      1000:64, -1:16,            0:16
              , 1:32,      -1:64,   8:16, "metadata", 3:16
              , 2:32,      -1:64,   -1:16,            3:16
       >>,
       #{api_version => 1})),

  ?assertEqual(
     {ok, #{topics => [#{name => <<"topic">>,
                         partitions_offset => [#{error_code => none,
                                                 metadata => <<>>,
                                                 offset => 1000,
                                                 partition => 0},
                                               #{error_code => unknown_topic_or_partition,
                                                 metadata => <<"metadata">>,
                                                 offset => -1,
                                                 partition => 1},
                                               #{error_code => unknown_topic_or_partition,
                                                 metadata => <<>>,
                                                 offset => -1,
                                                 partition => 2}]}],
            error_code => none}},
     kafe_protocol_consumer_offset_fetch:response(
       << 1:32              % topics count
            , 5:16, "topic" % topic name
            , 3:32          % partitions count
              % partition, offset,  metadata,         error code
              , 0:32,      1000:64, -1:16,            0:16
              , 1:32,      -1:64,   8:16, "metadata", 3:16
              , 2:32,      -1:64,   -1:16,            3:16
          , 0:16            % error code
       >>,
       #{api_version => 2})),

  ?assertEqual(
     {ok, #{throttle_time => 1234,
            topics => [#{name => <<"topic">>,
                         partitions_offset => [#{error_code => none,
                                                 metadata => <<>>,
                                                 offset => 1000,
                                                 partition => 0},
                                               #{error_code => unknown_topic_or_partition,
                                                 metadata => <<"metadata">>,
                                                 offset => -1,
                                                 partition => 1},
                                               #{error_code => unknown_topic_or_partition,
                                                 metadata => <<>>,
                                                 offset => -1,
                                                 partition => 2}]}],
            error_code => none}},
     kafe_protocol_consumer_offset_fetch:response(
       << 1234:32           % throttle time
          , 1:32            % topics count
            , 5:16, "topic" % topic name
            , 3:32          % partitions count
              % partition, offset,  metadata,         error code
              , 0:32,      1000:64, -1:16,            0:16
              , 1:32,      -1:64,   8:16, "metadata", 3:16
              , 2:32,      -1:64,   -1:16,            3:16
          , 0:16            % error code
       >>,
       #{api_version => 3})).
