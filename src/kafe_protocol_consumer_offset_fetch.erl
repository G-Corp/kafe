% @hidden
-module(kafe_protocol_consumer_offset_fetch).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/1
        ]).

run(ConsumerGroup, Options) ->
  case kafe:consumer_metadata(ConsumerGroup) of
    {ok, #{coordinator_host := BrokerName}} ->
      Options1 = if
                   Options =:= [] -> maps:keys(kafe:topics());
                   true -> Options
                 end,
      gen_server:call(kafe:broker_by_name(BrokerName),
                      {call,
                       fun ?MODULE:request/3, [ConsumerGroup, Options1],
                       fun ?MODULE:response/1},
                      infinity);
    E -> E
  end.

request(ConsumerGroup, Options, State) ->
  kafe_protocol:request(
    ?OFFSET_FETCH_REQUEST,
    <<(kafe_protocol:encode_string(ConsumerGroup))/binary, (topics(Options))/binary>>,
    State).

response(<<NumberOfTopics:32/signed, Remainder/binary>>) ->
  {ok, response(NumberOfTopics, Remainder)}.

% Private

topics(Options) ->
  topics(Options, <<(length(Options)):32/signed>>).

topics([], Result) -> Result;
topics([{TopicName, Partitions}|Rest], Result) ->
  topics(Rest,
         <<
           Result/binary,
           (kafe_protocol:encode_string(TopicName))/binary,
           (kafe_protocol:encode_array(
              [<<Partition:32/signed>> || Partition <- Partitions]
             ))/binary
         >>);
topics([TopicName|Rest], Result) ->
  topics([{TopicName, maps:keys(maps:get(TopicName, kafe:topics(), #{}))}|Rest], Result).

response(0, <<>>) ->
  [];
response(N,
         <<
           TopicNameLength:16/signed,
           TopicName:TopicNameLength/bytes,
           NumberOfPartitions:32/signed,
           PartitionsRemainder/binary
         >>) ->
  {PartitionsOffset, Remainder} = partitions_offset(NumberOfPartitions, PartitionsRemainder, []),
  [#{name => TopicName, partitions_offset => PartitionsOffset} | response(N - 1, Remainder)].

partitions_offset(0, Remainder, Acc) ->
  {Acc, Remainder};
partitions_offset(N,
                  <<
                    Partition:32/signed,
                    Offset:64/signed,
                    MetadataLength:16/signed,
                    Metadata:MetadataLength/bytes,
                    ErrorCode:16/signed,
                    Remainder/binary
                  >>,
                  Acc) ->
  partitions_offset(N - 1,
                    Remainder,
                    [#{partition => Partition,
                       offset => Offset,
                       metadata => Metadata,
                       error_code => kafe_error:code(ErrorCode)} | Acc]).

