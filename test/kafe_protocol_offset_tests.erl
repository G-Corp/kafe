-module(kafe_protocol_offset_tests).
-include_lib("eunit/include/eunit.hrl").
-include("../include/kafe.hrl").

kafe_protocol_offset_test_() ->
  {setup, fun setup/0, fun teardown/1,
   [
    ?_test(t_request()),
    ?_test(t_response())
   ]
  }.

setup() ->
  meck:new(kafe, [passthrough]),
  meck:expect(kafe, partitions, 1, [0, 1, 2]),
  ok.

teardown(_) ->
  meck:unload(kafe),
  ok.

t_request() ->
  ?assertEqual(
     #{packet => << ?OFFSET_REQUEST:16  % API key
                    , 0:16              % API version
                    , 0:32              % correlation ID
                    , 4:16, "test"      % topic
                      , -1:32             % replica ID
                      , 1:32              % topics count
                        , 5:16, "topic"   % topic name
                        , 3:32            % partitions count
                          % partition number, timestamp               , max offsets
                          , 0:32            , 18446744073709551615:64 , 65535:32
                          , 1:32            , 18446744073709551615:64 , 65535:32
                          , 2:32            , 18446744073709551615:64 , 65535:32
                 >>,
       state => #{api_key => ?OFFSET_REQUEST,
                  api_version => 0,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_offset:request(-1,
                                  [<<"topic">>],
                                  #{},
                                  #{api_key => ?OFFSET_REQUEST,
                                    api_version => 0,
                                    correlation_id => 0,
                                    client_id => <<"test">>})),
  ?assertEqual(
     #{packet => << ?OFFSET_REQUEST:16  % API key
                    , 1:16              % API version
                    , 0:32              % correlation ID
                    , 4:16, "test"      % topic
                      , -1:32             % replica ID
                      , 1:32              % topics count
                        , 5:16, "topic"   % topic name
                        , 3:32            % partitions count
                          % partition number, timestamp
                          , 0:32            , 18446744073709551615:64
                          , 1:32            , 18446744073709551615:64
                          , 2:32            , 18446744073709551615:64
                 >>,
       state => #{api_key => ?OFFSET_REQUEST,
                  api_version => 1,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_offset:request(-1,
                                  [<<"topic">>],
                                  #{},
                                  #{api_key => ?OFFSET_REQUEST,
                                    api_version => 1,
                                    correlation_id => 0,
                                    client_id => <<"test">>})),
  ?assertEqual(
     #{packet => << ?OFFSET_REQUEST:16  % API key
                    , 2:16              % API version
                    , 0:32              % correlation ID
                    , 4:16, "test"      % topic
                      , -1:32             % replica ID
                      , 1:8               % isolation level
                      , 1:32              % topics count
                        , 5:16, "topic"   % topic name
                        , 3:32            % partitions count
                          % partition number, timestamp
                          , 0:32            , 18446744073709551615:64
                          , 1:32            , 18446744073709551615:64
                          , 2:32            , 18446744073709551615:64
                 >>,
       state => #{api_key => ?OFFSET_REQUEST,
                  api_version => 2,
                  client_id => <<"test">>,
                  correlation_id => 1}},
     kafe_protocol_offset:request(-1,
                                  [<<"topic">>],
                                  #{},
                                  #{api_key => ?OFFSET_REQUEST,
                                    api_version => 2,
                                    correlation_id => 0,
                                    client_id => <<"test">>})).

t_response() ->
  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              id => 0,
                              offsets => [4, 0]}]}]},
     kafe_protocol_offset:response(
       <<1:32              % topics count
         , 5:16, "topic"   % topic
         , 1:32            % parition count
           , 0:32            % partition ID
           , 0:16            % error code
           , 2:32            % offsets count
             , 4:64, 0:64
       >>,
       %   0, 0, 0, 0, 0, 0, 0>>,
       #{api_version => 0})),

  ?assertEqual(
     {ok, [#{name => <<"topic">>,
             partitions => [#{error_code => none,
                              id => 0,
                              offset => 4,
                              timestamp => -1}]}]},
     kafe_protocol_offset:response(
       <<1:32             % topic count
         , 5:16, "topic"  % topic
         , 1:32           % partition count
           , 0:32           % partition ID
           , 0:16           % error code
           , -1:64          % timestamp
           , 4:64           % offset
       >>,
       #{api_version => 1})),

  ?assertEqual(
     {ok, #{throttle_time => 0,
            topics => [
                       #{name => <<"topic">>,
                         partitions => [#{error_code => none,
                                          id => 0,
                                          offset => 4,
                                          timestamp => -1}]}]}},
     kafe_protocol_offset:response(
       <<0:32                 % throttle time
         , 1:32               % topic count
           , 5:16, "topic"    % topic
           , 1:32             % partition count
             , 0:32           % partition ID
             , 0:16           % error code
             , -1:64          % timestamp
             , 4:64           % offset
       >>,
       #{api_version => 2})).
