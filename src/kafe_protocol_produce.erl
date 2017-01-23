% @hidden
-module(kafe_protocol_produce).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/2
        ]).

% [{broker_id, [{topic, [{partition, [message]}]}]}]
run(Messages, Options) ->
  case dispatch(Messages,
           maps:get(key_to_partition, Options, fun kafe:default_key_to_partition/2),
           []) of
    {ok, Dispatch} ->
      consolidate(
        [kafe_protocol:run(BrokerID,
                           {call,
                            fun ?MODULE:request/3, [Messages0, Options],
                            fun ?MODULE:response/2})
         || {BrokerID, Messages0} <- Dispatch]);
    {error, _} = Error ->
      Error
  end.

%% Produce Request (Version: 0) => acks timeout [topic_data]
%%   acks => INT16
%%   timeout => INT32
%%   topic_data => topic [data]
%%     topic => STRING
%%     data => partition record_set
%%       partition => INT32
%%       record_set => BYTES
%% Options:
%%   * timeout :: integer()       (default: 5000)
%%   * required_acks :: integer() (default: -1)
%%   * partition :: integer()     (default: 0)
%%   * timestamp :: integer()     (default: now)
%%
%% Message Set:
%% v0
%% Message => Crc MagicByte Attributes Key Value
%%   Crc => int32
%%   MagicByte => int8
%%   Attributes => int8
%%   Key => bytes
%%   Value => bytes
%%
%% v1 (supported since 0.10.0)
%% Message => Crc MagicByte Attributes Timestamp Key Value
%%   Crc => int32
%%   MagicByte => int8
%%   Attributes => int8
%%   Timestamp => int64
%%   Key => bytes
%%   Value => bytes
request(Messages, Options, #{api_version := ApiVersion} = State) ->
  Timeout = maps:get(timeout, Options, ?DEFAULT_PRODUCE_SYNC_TIMEOUT),
  RequiredAcks = maps:get(required_acks,
                          Options,
                          ?DEFAULT_PRODUCE_REQUIRED_ACKS),
  Encoded = encode_messages_topics(Messages, Options, ApiVersion, []),
  kafe_protocol:request(
    ?PRODUCE_REQUEST,
    <<RequiredAcks:16, Timeout:32, Encoded/binary>>,
    State,
    ApiVersion).

encode_messages_topics([], _, _, Acc) ->
  kafe_protocol:encode_array(lists:reverse(Acc));
encode_messages_topics([{Topic, Messages}|Rest], Options, ApiVersion, Acc) ->
  encode_messages_topics(
    Rest,
    Options,
    ApiVersion,
    [<<(kafe_protocol:encode_string(Topic))/binary,
       (encode_messages_partitions(Messages, Options, ApiVersion, []))/binary>>
     |Acc]).

encode_messages_partitions([], _, _, Acc) ->
  kafe_protocol:encode_array(lists:reverse(Acc));
encode_messages_partitions([{Partition, Messages}|Rest], Options, ApiVersion, Acc) ->
  MessageSet = message_set(Messages, Options, ApiVersion, <<>>),
  encode_messages_partitions(
    Rest,
    Options,
    ApiVersion,
    [<<Partition:32/signed,
       (kafe_protocol:encode_bytes(MessageSet))/binary>>
     |Acc]).

message_set([], _, _, Result) ->
  Result;
message_set([{Key, Value}|Rest], Options, ApiVersion, Acc) ->
  Msg = if
          ApiVersion >= ?V2 ->
            Timestamp = maps:get(timestamp, Options, get_timestamp()),
            <<
              1:8/signed, % MagicByte
              0:8/signed, % Attributes
              Timestamp:64/signed, % Timestamp
              (kafe_protocol:encode_bytes(bucs:to_binary(Key)))/binary, % Key
              (kafe_protocol:encode_bytes(bucs:to_binary(Value)))/binary % Value
            >>;
          true ->
            <<
              0:8/signed, % MagicByte
              0:8/signed, % Attributes
              (kafe_protocol:encode_bytes(bucs:to_binary(Key)))/binary, % Key
              (kafe_protocol:encode_bytes(bucs:to_binary(Value)))/binary % Value
            >>
        end,
  SignedMsg = <<(erlang:crc32(Msg)):32/signed, Msg/binary>>,
  message_set(Rest, Options, ApiVersion,
              <<Acc/binary,
                0:64/signed, % Offset
                (kafe_protocol:encode_bytes(SignedMsg))/binary>>). % MessageSize Message

%% Produce Response
%% v0
%% ProduceResponse => [TopicName [Partition ErrorCode Offset]]
%%   TopicName => string
%%   Partition => int32
%%   ErrorCode => int16
%%   Offset => int64
%%
%% v1 (supported in 0.9.0 or later)
%% ProduceResponse => [TopicName [Partition ErrorCode Offset]] ThrottleTime
%%   TopicName => string
%%   Partition => int32
%%   ErrorCode => int16
%%   Offset => int64
%%   ThrottleTime => int32
%%
%% v2 (supported in 0.10.0 or later)
%% ProduceResponse => [TopicName [Partition ErrorCode Offset Timestamp]] ThrottleTime
%%   TopicName => string
%%   Partition => int32
%%   ErrorCode => int16
%%   Offset => int64
%%   Timestamp => int64
%%   ThrottleTime => int32
response(<<NumberOfTopics:32/signed, Remainder/binary>>, ApiVersion) ->
  {ok, response(NumberOfTopics, Remainder, ApiVersion)}.

% Private

% v0
response(0, _, ApiVersion) when ApiVersion == ?V0 ->
  [];
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary
  >>,
  ApiVersion) when ApiVersion == ?V0 ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionRemainder, [], ApiVersion),
  [#{name => TopicName,
     partitions => Partitions} | response(N - 1, Remainder, 0)];
% v1 & v2
response(N, Remainder, ApiVersion) when ApiVersion == ?V1;
                                        ApiVersion == ?V2 ->
  {Topics, <<ThrottleTime:32/signed, _/binary>>} = response(N, Remainder, ApiVersion, []),
  #{topics => Topics,
    throttle_time => ThrottleTime}.

response(0, Remainder, ApiVersion, Acc) when ApiVersion == ?V1;
                                             ApiVersion == ?V2 ->
  {Acc, Remainder};
response(
  N,
  <<
    TopicNameLength:16/signed,
    TopicName:TopicNameLength/bytes,
    NumberOfPartitions:32/signed,
    PartitionRemainder/binary
  >>,
  ApiVersion,
  Acc) when ApiVersion == ?V1;
            ApiVersion == ?V2 ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionRemainder, [], ApiVersion),
  response(N - 1, Remainder, ApiVersion, [#{name => TopicName,
                                            partitions => Partitions} | Acc]).

% v0 & v1
partitions(0, Remainder, Acc, ApiVersion) when ApiVersion == ?V0;
                                               ApiVersion == ?V1 ->
  {Acc, Remainder};
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    Offset:64/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V0;
                   ApiVersion == ?V1 ->
  partitions(N - 1,
             Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                offset => Offset} | Acc], ApiVersion);
partitions(0, Remainder, Acc, ApiVersion) when ApiVersion == ?V2 ->
  {Acc, Remainder};
% v2
partitions(
  N,
  <<
    Partition:32/signed,
    ErrorCode:16/signed,
    Offset:64/signed,
    Timestamp:64/signed,
    Remainder/binary
  >>,
  Acc,
  ApiVersion) when ApiVersion == ?V2 ->
  partitions(N - 1,
             Remainder,
             [#{partition => Partition,
                error_code => kafe_error:code(ErrorCode),
                offset => Offset,
                timestamp => Timestamp} | Acc], ApiVersion).

get_timestamp() ->
  {Mega, Sec, Micro} = erlang:timestamp(),
  (Mega * 1000000 + Sec) * 1000000 + Micro.

% Message dispatch per brocker
dispatch([], _, Result) ->
  {ok, Result};
dispatch([{Topic, Messages}|Rest], KeyToPartition, Result) ->
  case dispatch(Messages, Topic, KeyToPartition, Result) of
    {error, _} = Error ->
      Error;
    Dispatch ->
      dispatch(Rest, KeyToPartition, Dispatch)
  end.

dispatch([], _, _, Result) ->
  Result;
dispatch([{Key, Value, Partition}|Rest], Topic, KeyToPartition, Result) when is_binary(Value),
                                                                             is_integer(Partition) ->
  case kafe_brokers:broker_id_by_topic_and_partition(Topic, Partition) of
    undefined ->
      {error, {Topic, Partition}};
    BrokerID ->
      TopicsForBroker = buclists:keyfind(BrokerID, 1, Result, []),
      PartitionsForTopic = buclists:keyfind(Topic, 1, TopicsForBroker, []),
      MessagesForPartition = buclists:keyfind(Partition, 1, PartitionsForTopic, []),
      dispatch(
        Rest,
        Topic,
        KeyToPartition,
        buclists:keyupdate(
          BrokerID,
          1,
          Result,
          {BrokerID,
           buclists:keyupdate(
             Topic,
             1,
             TopicsForBroker,
             {Topic,
              buclists:keyupdate(
                Partition,
                1,
                PartitionsForTopic,
                {Partition,
                 MessagesForPartition ++ [{Key, Value}]})})}))
  end;
dispatch([{Key, Value}|Rest], Topic, KeyToPartition, Result) when is_binary(Value) ->
  dispatch([{Key, Value, erlang:apply(KeyToPartition, [Topic, Key])}|Rest], Topic, KeyToPartition, Result);
dispatch([{Value, Partition}|Rest], Topic, KeyToPartition, Result) when is_binary(Value),
                                                                        is_integer(Partition) ->
  dispatch([{<<>>, Value, Partition}|Rest], Topic, KeyToPartition, Result);
dispatch([Value|Rest], Topic, KeyToPartition, Result) when is_binary(Value) ->
  dispatch([{<<>>, Value, kafe_rr:next(Topic)}|Rest], Topic, KeyToPartition, Result).

consolidate([{ok, Base}|Rest]) ->
  consolidate(Rest, Base).

consolidate([], Acc) ->
  {ok, Acc};
consolidate([{ok, #{topics := Topics}}|Rest], #{topics := AccTopics} = Acc) ->
  consolidate(
    Rest,
    Acc#{topics => consolidate_topics(Topics, AccTopics)});
consolidate([{error, _} = Error|_], _) ->
  Error.

consolidate_topics([], Topics) ->
  Topics;
consolidate_topics([#{name := Topic, partitions := Partitions}|Rest], Topics) ->
  consolidate_topics(
    Rest,
    add_partition(Topics, Topic, Partitions, [])).

add_partition([], _, [], Acc) ->
  Acc;
add_partition([], Topic, Partitions, Acc) ->
  [#{name => Topic, partitions => Partitions}|Acc];
add_partition([#{name := Topic, partitions := CurrentPartitions}|Rest], Topic, Partitions, Acc) ->
  add_partition(Rest, Topic, [], [#{name => Topic, partitions => CurrentPartitions ++ Partitions}|Acc]);
add_partition([Current|Rest], Topic, Partitions, Acc) ->
  add_partition(Rest, Topic, Partitions, [Current|Acc]).

