-module(kafe_consumer_subscriber).
-behaviour(gen_server).
-include("../include/kafe_consumer.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% Copied from bucs due to Elixir 1.3 compilation error
-define(RECORD_TO_LIST(Record, Val),
        begin
            Fields = record_info(fields, Record),
            [_Tag| Values] = tuple_to_list(Val),
            lists:zip(Fields, Values)
        end).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-export([message/2]).

-record(state, {
          group_id,
          topic,
          partition,
          subscriber_module,
          subscriber_args,
          subscriber_state
         }).

% Initialize the consumer group subscriber
-callback init(Group :: binary(),
               Topic :: binary(),
               Partition :: integer(),
               Args :: list(term())) ->
  {ok, State :: term()}
  | {error, Reason :: term()}.

% Handle a message
%
% Return :
%
% <tt>{ok, NewState :: term()}</tt> : The message has been received.
%
% <tt>{stop, Reason :: term(), NewState :: term()} : The message has been received, but the consumer must stop.
-callback handle_message(Message :: message(),
                         State :: term()) ->
  {ok, NewState :: term()}
  | {error, Reason :: term(), NewState :: term()}.

% @doc
% Get message attributes.
%
% Example:
% <pre>
% Topic = kafe_consumer_subscriber:message(Message, topic).
% </pre>
% @end
message(Message, Field) ->
  case lists:keyfind(Field, 1, ?RECORD_TO_LIST(message, Message)) of
    {Field, Value} ->
      Value;
    false ->
      undefined
  end.

% @hidden
start_link({Module, Args}, GroupID, Topic, Partition) ->
  gen_server:start_link(?MODULE, [Module, Args, GroupID, Topic, Partition], []);
start_link(Module, GroupID, Topic, Partition) ->
  gen_server:start_link(?MODULE, [Module, [], GroupID, Topic, Partition], []).

% @hidden
init([SubscriberModule, SubscriberArgs, GroupID, Topic, Partition]) ->
  erlang:process_flag(trap_exit, true),
  case erlang:apply(SubscriberModule, init, [GroupID, Topic, Partition, SubscriberArgs]) of
    {ok, SubscriberState} ->
      kafe_consumer_store:insert(GroupID, {subscriber_pid, {Topic, Partition}}, self()),
      {ok, #state{
              group_id = GroupID,
              topic = Topic,
              partition = Partition,
              subscriber_module = SubscriberModule,
              subscriber_args = SubscriberArgs,
              subscriber_state = SubscriberState
             }};
    {stop, Reason} ->
      {stop, Reason}
  end.

% @hidden
handle_call({message, CommitRef, Topic, Partition, Offset, Key, Value},
            _From,
            #state{topic = Topic,
                   partition = Partition,
                   subscriber_module = SubscriberModule,
                   subscriber_state = SubscriberState} = State) ->
  case erlang:apply(SubscriberModule,
                    handle_message,
                    [#message{commit_ref = CommitRef,
                              topic = Topic,
                              partition = Partition,
                              offset = Offset,
                              key = Key,
                              value = Value},
                     SubscriberState]) of
    {error, Reason, NewSubscriberState} ->
      {reply, {error, Reason}, State#state{subscriber_state = NewSubscriberState}};
    {ok, NewSubscriberState} ->
      {reply, ok, State#state{subscriber_state = NewSubscriberState}}
  end;
handle_call(_Request, _From, State) ->
  {reply, {error, invalid_message}, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, #state{group_id = GroupID, topic = Topic, partition = Partition}) ->
  kafe_consumer_store:delete(GroupID, {subscriber_pid, {Topic, Partition}}),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

-ifdef(TEST).
message_test() ->
  Message = #message{commit_ref = <<"COMMITREF">>,
                     topic = <<"topic">>,
                     partition = 1,
                     offset = 1234,
                     key = <<"key">>,
                     value = <<"value">>},
  ?assertEqual(<<"COMMITREF">>, message(Message, commit_ref)),
  ?assertEqual(<<"topic">>, message(Message, topic)),
  ?assertEqual(1, message(Message, partition)),
  ?assertEqual(<<"key">>, message(Message, key)),
  ?assertEqual(<<"value">>, message(Message, value)),
  ?assertEqual(undefined, message(Message, invalid_field)).

init_ok_test() ->
  meck:new(test_subscriber, [non_strict]),
  meck:expect(test_subscriber, init, 4, {ok, subscriber_state_for_test}),
  ?assertEqual({ok, #state{
                       group_id = <<"group">>,
                       topic = <<"topic">>,
                       partition = 1,
                       subscriber_module = test_subscriber,
                       subscriber_args = subscriber_args_for_test,
                       subscriber_state = subscriber_state_for_test
                      }},
               init([test_subscriber,
                     subscriber_args_for_test,
                     <<"group">>,
                     <<"topic">>,
                     1])),
  meck:unload(test_subscriber).

init_stop_test() ->
  meck:new(test_subscriber, [non_strict]),
  meck:expect(test_subscriber, init, 4, {stop, test_stop}),
  ?assertEqual({stop, test_stop},
               init([test_subscriber,
                     subscriber_args_for_test,
                     <<"group">>,
                     <<"topic">>,
                     1])),
  meck:unload(test_subscriber).

handle_message_test() ->
  meck:new(test_subscriber, [non_strict]),
  meck:expect(test_subscriber, handle_message, 2, {ok, new_subscriber_state_for_test}),
  ?assertEqual({reply, ok, #state{
                              group_id = <<"group">>,
                              topic = <<"topic">>,
                              partition = 1,
                              subscriber_module = test_subscriber,
                              subscriber_args = subscriber_args_for_test,
                              subscriber_state = new_subscriber_state_for_test
                             }},
               handle_call({message,
                            <<"COMMITREF">>,
                            <<"topic">>,
                            1,
                            1234,
                            <<"key">>,
                            <<"value">>},
                           from,
                           #state{
                              group_id = <<"group">>,
                              topic = <<"topic">>,
                              partition = 1,
                              subscriber_module = test_subscriber,
                              subscriber_args = subscriber_args_for_test,
                              subscriber_state = subscriber_state_for_test
                             })),
  meck:unload(test_subscriber).

handle_message_subscriber_error_test() ->
  meck:new(test_subscriber, [non_strict]),
  meck:expect(test_subscriber, handle_message, 2, {error, subscriber_error, new_subscriber_state_for_test}),
  ?assertEqual({reply,
                {error, subscriber_error},
                #state{
                   group_id = <<"group">>,
                   topic = <<"topic">>,
                   partition = 1,
                   subscriber_module = test_subscriber,
                   subscriber_args = subscriber_args_for_test,
                   subscriber_state = new_subscriber_state_for_test
                  }},
               handle_call({message,
                            <<"COMMITREF">>,
                            <<"topic">>,
                            1,
                            1234,
                            <<"key">>,
                            <<"value">>},
                           from,
                           #state{
                              group_id = <<"group">>,
                              topic = <<"topic">>,
                              partition = 1,
                              subscriber_module = test_subscriber,
                              subscriber_args = subscriber_args_for_test,
                              subscriber_state = subscriber_state_for_test
                             })),
  meck:unload(test_subscriber).

handle_message_invalid_message_topic_test() ->
  ?assertEqual({reply,
                {error, invalid_message},
                #state{
                   group_id = <<"group">>,
                   topic = <<"topic">>,
                   partition = 1,
                   subscriber_module = test_subscriber,
                   subscriber_args = subscriber_args_for_test,
                   subscriber_state = subscriber_state_for_test
                  }},
               handle_call({message,
                            <<"COMMITREF">>,
                            <<"topic1">>,
                            1,
                            1234,
                            <<"key">>,
                            <<"value">>},
                           from,
                           #state{
                              group_id = <<"group">>,
                              topic = <<"topic">>,
                              partition = 1,
                              subscriber_module = test_subscriber,
                              subscriber_args = subscriber_args_for_test,
                              subscriber_state = subscriber_state_for_test
                             })).

handle_message_invalid_message_partition_test() ->
  ?assertEqual({reply,
                {error, invalid_message},
                #state{
                   group_id = <<"group">>,
                   topic = <<"topic">>,
                   partition = 1,
                   subscriber_module = test_subscriber,
                   subscriber_args = subscriber_args_for_test,
                   subscriber_state = subscriber_state_for_test
                  }},
               handle_call({message,
                            <<"COMMITREF">>,
                            <<"topic">>,
                            2,
                            1234,
                            <<"key">>,
                            <<"value">>},
                           from,
                           #state{
                              group_id = <<"group">>,
                              topic = <<"topic">>,
                              partition = 1,
                              subscriber_module = test_subscriber,
                              subscriber_args = subscriber_args_for_test,
                              subscriber_state = subscriber_state_for_test
                             })).

-endif.

