% @hidden
-module(kafe_consumer_store).

-export([
         new/1,
         exist/1,
         insert/3,
         lookup/2,
         value/2,
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

