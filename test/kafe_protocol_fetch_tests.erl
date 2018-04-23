-module(kafe_protocol_fetch_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_fetch_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request())
    , ?_test(t_response())
    , ?_test(t_incomplete_response())
    , ?_test(t_response_incomplete_final_message_is_ignored())
   ]
  }.

setup() ->
  ok.

teardown(_) ->
  ok.

t_request() ->
  ?assertEqual(
     #{packet => << ?FETCH_REQUEST:16                 % API Key
                    , 0:16                            % API version
                    , 0:32                            % correlation ID
                    , 4:16, "test"                    % client ID
                      , -1:32                           % replica ID
                      , ?DEFAULT_FETCH_MAX_WAIT_TIME:32 % max wait time
                      , ?DEFAULT_FETCH_MIN_BYTES:32     % min bytes
                      , 1:32                            % topics count
                        , 5:16, "topic"                   % topic name
                        , 1:32                            % partition count
                          , 0:32                            % partition index
                          , 1:64                            % fetch offset
                          , (?DEFAULT_FETCH_MAX_BYTES):32     % max bytes
                 >>,
       state => #{api_key => ?FETCH_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_fetch:request(-1,
                                 [{<<"topic">>, [{0, 1, ?DEFAULT_FETCH_MAX_BYTES}]}],
                                 #{},
                                 #{api_key => ?FETCH_REQUEST,
                                   api_version => 0,
                                   correlation_id => 0,
                                   client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?FETCH_REQUEST:16                 % API Key
                    , 1:16                            % API version
                    , 0:32                            % correlation ID
                    , 4:16, "test"                    % client ID
                      , -1:32                           % replica ID
                      , ?DEFAULT_FETCH_MAX_WAIT_TIME:32 % max wait time
                      , ?DEFAULT_FETCH_MIN_BYTES:32     % min bytes
                      , 1:32                            % topics count
                        , 5:16, "topic"                   % topic name
                        , 1:32                            % partition count
                          , 0:32                            % partition index
                          , 1:64                            % fetch offset
                          , (?DEFAULT_FETCH_MAX_BYTES):32     % max bytes
                 >>,
       state => #{api_key => ?FETCH_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_fetch:request(-1,
                                 [{<<"topic">>, [{0, 1, ?DEFAULT_FETCH_MAX_BYTES}]}],
                                 #{},
                                 #{api_key => ?FETCH_REQUEST,
                                   api_version => 1,
                                   correlation_id => 0,
                                   client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?FETCH_REQUEST:16                 % API Key
                    , 2:16                            % API version
                    , 0:32                            % correlation ID
                    , 4:16, "test"                    % client ID
                      , -1:32                           % replica ID
                      , ?DEFAULT_FETCH_MAX_WAIT_TIME:32 % max wait time
                      , ?DEFAULT_FETCH_MIN_BYTES:32     % min bytes
                      , 1:32                            % topics count
                        , 5:16, "topic"                   % topic name
                        , 1:32                            % partition count
                          , 0:32                            % partition index
                          , 1:64                            % fetch offset
                          , (?DEFAULT_FETCH_MAX_BYTES):32     % max bytes
                 >>,
       state => #{api_key => ?FETCH_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_fetch:request(-1,
                                 [{<<"topic">>, [{0, 1, ?DEFAULT_FETCH_MAX_BYTES}]}],
                                 #{},
                                 #{api_key => ?FETCH_REQUEST,
                                   api_version => 2,
                                   correlation_id => 0,
                                   client_id => <<"test">>})),

  ?assertEqual(
     #{packet => << ?FETCH_REQUEST:16                 % API Key
                    , 3:16                            % API version
                    , 0:32                            % correlation ID
                    , 4:16, "test"                    % client ID
                      , -1:32                           % replica ID
                      , ?DEFAULT_FETCH_MAX_WAIT_TIME:32 % max wait time
                      , ?DEFAULT_FETCH_MIN_BYTES:32     % min bytes
                      , (?DEFAULT_FETCH_MAX_BYTES):32   % max bytes
                      , 1:32                            % topics count
                        , 5:16, "topic"                   % topic name
                        , 1:32                            % partition count
                          , 0:32                            % partition index
                          , 1:64                            % fetch offset
                          , (?DEFAULT_FETCH_MAX_BYTES):32     % max bytes
                 >>,
       state => #{api_key => ?FETCH_REQUEST,
                  api_version => 3,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_fetch:request(-1,
                                 [{<<"topic">>, [{0, 1, ?DEFAULT_FETCH_MAX_BYTES}]}],
                                 #{},
                                 #{api_key => ?FETCH_REQUEST,
                                   api_version => 3,
                                   correlation_id => 0,
                                   client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              high_watermark_offset => 5,
                              messages => [#{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 0,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 1,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 2,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 3,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 4,
                                             value => <<"hello world">>}],
                              partition => 0}]}]},
     kafe_protocol_fetch:response(
       <<1:32                       % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+25)*5):32      % message set size
               % offset, size, crc, magic, attributes, key, value
               , 0:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
               , 1:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
               , 2:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
               , 3:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
               , 4:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 0})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics =>[#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 1})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics =>[#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 2})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics =>[#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 3})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics => [#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         last_stable_offset => 5,
                                         aborted_transactions => [#{producer_id => 1,
                                                                    first_offset => 100},
                                                                  #{producer_id => 2,
                                                                    first_offset => 200}],
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , 5:64                 % last stable offset
             , 2:32                 % aborted transactions count
               % producer ID, first offet
               , 1:64       , 100:64
               , 2:64       , 200:64
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 4})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics => [#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         last_stable_offset => 5,
                                         log_start_offset => 0,
                                         aborted_transactions => [#{producer_id => 1,
                                                                    first_offset => 100},
                                                                  #{producer_id => 2,
                                                                    first_offset => 200}],
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , 5:64                 % last stable offset
             , 0:64                 % log start offset
             , 2:32                 % aborted transactions count
               % producer ID, first offet
               , 1:64       , 100:64
               , 2:64       , 200:64
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 5})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics => [#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         last_stable_offset => 5,
                                         log_start_offset => 0,
                                         aborted_transactions => [#{producer_id => 1,
                                                                    first_offset => 100},
                                                                  #{producer_id => 2,
                                                                    first_offset => 200}],
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , 5:64                 % last stable offset
             , 0:64                 % log start offset
             , 2:32                 % aborted transactions count
               % producer ID, first offet
               , 1:64       , 100:64
               , 2:64       , 200:64
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 6})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics => [#{name => <<"topic">>,
                        partitions => [#{error_code => none,
                                         high_watermark_offset => 5,
                                         last_stable_offset => 5,
                                         log_start_offset => 0,
                                         aborted_transactions => [#{producer_id => 1,
                                                                    first_offset => 100},
                                                                  #{producer_id => 2,
                                                                    first_offset => 200}],
                                         messages => [#{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 0,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 1,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 2,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 3,
                                                        timestamp => -1,
                                                        value => <<"hello world">>},
                                                      #{attributes => 0,
                                                        crc => 1940715388,
                                                        key => <<>>,
                                                        magic_byte => 1,
                                                        offset => 4,
                                                        timestamp => -1,
                                                        value => <<"hello world">>}],
                                         partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , 5:64                 % last stable offset
             , 0:64                 % log start offset
             , 2:32                 % aborted transactions count
               % producer ID, first offet
               , 1:64       , 100:64
               , 2:64       , 200:64
             , ((8+4+33)*5):32      % message set size
               % offset, size, crc, magic, attributes, timestamp, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 2:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 3:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 4:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
       >>,
       #{api_version => 7})).

t_incomplete_response() ->
  ?assertEqual({error, incomplete_data},
               kafe_protocol_fetch:response(<<>>,
                                            #{api_version => 0})),
  ?assertEqual({error, incomplete_data},
               kafe_protocol_fetch:response(<<0>>,
                                            #{api_version => 0})),
  ?assertEqual({error, incomplete_data},
               kafe_protocol_fetch:response(
                 <<1:32                       % topic count
                   , 5:16, "topic"          % topic name
                   , 1:32                   % partition count
                   , 0:32                 % partition index
                   , 0:16                 % error code
                   , 5:64                 % highwater mark
                   , ((8+4+25)*5):32      % message set size
                   , 0 % message set truncated, does not match size
                 >>,
                 #{api_version => 0})).

t_response_incomplete_final_message_is_ignored() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              high_watermark_offset => 5,
                              messages => [#{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 0,
                                             value => <<"hello world">>},
                                           #{attributes => 0,
                                             crc => 1940715388,
                                             key => <<>>,
                                             magic_byte => 0,
                                             offset => 1,
                                             value => <<"hello world">>}
                                          ],
                              partition => 0}]}]},
     kafe_protocol_fetch:response(
       <<1:32                       % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+25)*2 + 1):32  % message set size
               % offset, size, crc, magic, attributes, key, value
               , 0:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
               , 1:64, 25:32, 1940715388:32, 0, 0, -1:32, 11:32, "hello world"
               , 0 % message truncated after 1 byte
       >>,
       #{api_version =>0})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics => [#{name => <<"topic">>,
                         partitions => [#{error_code => none,
                                          high_watermark_offset => 5,
                                          messages => [#{attributes => 0,
                                                         crc => 1940715388,
                                                         key => <<>>,
                                                         magic_byte => 1,
                                                         offset => 0,
                                                         timestamp => -1,
                                                         value => <<"hello world">>},
                                                       #{attributes => 0,
                                                         crc => 1940715388,
                                                         key => <<>>,
                                                         magic_byte => 1,
                                                         offset => 1,
                                                         timestamp => -1,
                                                         value => <<"hello world">>}
                                                      ],
                                          partition => 0}]}]}},
     kafe_protocol_fetch:response(
       <<0:32                       % throttle time ms
         , 1:32                     % topic count
           , 5:16, "topic"          % topic name
           , 1:32                   % partition count
             , 0:32                 % partition index
             , 0:16                 % error code
             , 5:64                 % highwater mark
             , ((8+4+33)*2 + 1):32  % message set size
               % offset, size, crc, magic, attributes, key, value
               , 0:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 1:64, 33:32, 1940715388:32, 1, 0, -1:64, -1:32, 11:32, "hello world"
               , 0 % message truncated after 1 byte
       >>,
       #{api_version => 1})).
