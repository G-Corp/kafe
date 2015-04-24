% @hidden
-module(kafe_protocol_offset).

-include("../include/kafe.hrl").

-export([
         run/2,
         request/3,
         response/1
        ]).

run(ReplicaID, Topics) ->
  maps:fold(
    fun(K, V, Acc) ->
        [#{name => K, partitions => V}|Acc]
    end, 
    [], 
    lists:foldl(
      fun(#{name := Name, partitions := Partitions}, Acc) ->
          case maps:get(Name, Acc, undefined) of
            undefined -> 
              maps:put(Name, Partitions, Acc);
            Data ->
              maps:put(Name, Data ++ Partitions, Acc)
          end
      end, 
      #{}, 
      lists:flatten(
        maps:fold(
          fun(Broker, TopicsForBroker, Acc) ->
              {ok, Result} = gen_server:call(Broker,
                                             {call, 
                                              fun ?MODULE:request/3, [ReplicaID, TopicsForBroker],
                                              fun ?MODULE:response/1}, 
                                             infinity),
              [Result|Acc]
          end, [], dispatch(Topics, kafe:topics()))
       ))).

request(ReplicaId, Topics, State) ->
  kafe_protocol:request(
    ?OFFSET_REQUEST, 
    <<ReplicaId:32/signed, (topics(Topics))/binary>>,
    State).

response(<<NumberOfTopics:32/signed, Remainder/binary>>) ->
  {ok, response(NumberOfTopics, Remainder)}.

% Private

dispatch(Topics, TopicsInfos) ->
  dispatch(Topics, TopicsInfos, #{}).

dispatch([], _, Result) -> Result;
dispatch([{Topic, Partitions}|Rest], TopicsInfos, Result) ->
  dispatch(Rest, 
           TopicsInfos,
           lists:foldl(fun({ID, _, _} = Partition, Acc) ->
                           Broker = kafe:broker(Topic, ID),
                           Topics = maps:get(Broker, Acc, []),
                           maps:put(Broker, [{Topic, [Partition]}|Topics], Acc)
                       end, Result, Partitions));
dispatch([Topic|Rest], TopicsInfos, Result) when is_binary(Topic) ->
  Partitions = lists:foldl(fun(Partition, Acc) ->
                               [{Partition, ?DEFAULT_OFFSET_TIME, ?DEFAULT_OFFSET_MAX_SIZE}|Acc]
                           end, [], maps:keys(maps:get(Topic, TopicsInfos))),
  dispatch([{Topic, Partitions}|Rest], TopicsInfos, Result).

topics(Topics) ->
  topics(Topics, <<(length(Topics)):32/signed>>).

topics([], Acc) -> Acc;
topics([{TopicName, Partitions} | T], Acc) ->
  topics(T,
         <<
           Acc/binary, 
           (kafe_protocol:encode_string(TopicName))/binary,
           (kafe_protocol:encode_array(
              [<<Partition:32/signed, FetchOffset:64/signed, MaxBytes:32/signed>> || 
               {Partition, FetchOffset, MaxBytes} <- Partitions]))/binary
         >>);
topics([TopicName | T], Acc) ->
  topics([{TopicName, [{?DEFAULT_OFFSET_PARTITION, 
                        ?DEFAULT_OFFSET_TIME, 
                        ?DEFAULT_OFFSET_MAX_SIZE}]} | T], Acc).

response(0, <<>>) ->
    [];
response(
  N, 
  <<
    TopicNameLength:16/signed, 
    TopicName:TopicNameLength/bytes, 
    NumberOfPartitions:32/signed, 
    PartitionsRemainder/binary
  >>) ->
  {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionsRemainder, []),
  [#{name => TopicName, partitions => Partitions} | response(N - 1, Remainder)].

partitions(0, Remainder, Acc) ->
    {Acc, Remainder};
partitions(
  N,
  <<
    Partition:32/signed, 
    ErrorCode:16/signed, 
    NumberOfOffsets:32/signed, 
    Offsets:NumberOfOffsets/binary-unit:64,
    Remainder/binary
  >>, 
  Acc) ->
  partitions(N - 1, 
             Remainder, 
             [#{id => Partition, 
                error_code => kafe_error:code(ErrorCode), 
                offsets => [Offset || <<Offset:64/signed>> <= Offsets]} | Acc]).

