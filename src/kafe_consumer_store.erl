% @hidden
% The store contain, for each consumer group :
% - sup_pid : the supervisor (kafe_consumer) PID
% - server_pid : the kafe_consumer_srv PID
% - fsm_pid : the kafe_consumer_fsm PID
% - member_id : the kafka member ID
% - generation_id : the Kafka generation ID
% - can_fetch : boolean
% - topics : a list of {topic, partition} for the consumer group
-module(kafe_consumer_store).

-export([
         new/1,
         exist/1,
         insert/3,
         append/3,
         lookup/2,
         value/2,
         value/3,
         count/1,
         delete/1,
         delete/2
        ]).

-define(IF_EXIST(Group, Exec, Else), case exist(Group) of
                                       true -> Exec;
                                       false -> Else
                                     end).

new(ConsumerGroup) ->
  ?IF_EXIST(ConsumerGroup,
            ok,
            begin
              ets:new(store(ConsumerGroup), [public, named_table]),
              ok
            end).

exist(ConsumerGroup) ->
  ets:info(store(ConsumerGroup)) /= undefined.

insert(ConsumerGroup, Key, Value) ->
  ?IF_EXIST(ConsumerGroup,
            ok,
            new(ConsumerGroup)),
  ets:insert(store(ConsumerGroup), {Key, Value}),
  ok.

append(ConsumerGroup, Key, Value) ->
  kafe_consumer_store:insert(
    ConsumerGroup,
    Key,
    lists:append(
      kafe_consumer_store:value(ConsumerGroup, Key, []),
      [Value])).

lookup(ConsumerGroup, Key) ->
  ?IF_EXIST(ConsumerGroup,
            case ets:lookup(store(ConsumerGroup), Key) of
              [{Key, Value}] ->
                {ok, Value};
              _ ->
                {error, undefined}
            end,
            {error, undefined}).

value(ConsumerGroup, Key) ->
  case lookup(ConsumerGroup, Key) of
    {ok, Value} -> Value;
    {error, undefined} -> undefined
  end.

value(ConsumerGroup, Key, Default) ->
  case lookup(ConsumerGroup, Key) of
    {ok, Value} -> Value;
    {error, undefined} -> Default
  end.

count(ConsumerGroup) ->
  ?IF_EXIST(ConsumerGroup,
            ets:info(store(ConsumerGroup), size),
            0).

delete(ConsumerGroup) ->
  ?IF_EXIST(ConsumerGroup,
            begin
              ets:delete(store(ConsumerGroup)),
              ok
            end,
            ok).

delete(ConsumerGroup, Key) ->
  ?IF_EXIST(ConsumerGroup,
            begin
              ets:delete(store(ConsumerGroup), Key),
              ok
            end,
            ok).

store(ConsumerGroup) ->
  bucs:to_atom(<<"kafe_", (bucs:to_binary(ConsumerGroup))/binary, "_store">>).

