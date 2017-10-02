-module(sample_logger_consumer).
-behaviour(kafe_consumer_subscriber).
-compile([{parse_transform, lager_transform}]).

-export([init/4, handle_message/2]).
-include_lib("kafe/include/kafe_consumer.hrl").

-record(state, {
          group,
          topic,
          partition,
          args
         }).

init(Group, Topic, Partition, Args) ->
  {ok, #state{
          group = Group,
          topic = Topic,
          partition = Partition,
          args = Args
         }}.

handle_message(Message, #state{group = Group, topic = Topic, partition = Partition, args = Args} = State) ->
  lager:info("[~p] ~p/~p (~p): ~p", [Group, Topic, Partition, Args, Message]),
  {ok, State}.
