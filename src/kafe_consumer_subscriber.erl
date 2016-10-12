-module(kafe_consumer_subscriber).
-behaviour(gen_server).

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-record(state, {
          group_id,
          topic,
          partition,
          commit_store_key,
          subscriber_module,
          subscriber_args,
          subscriber_state
         }).

-record(message, {
          commit_ref,
          topic,
          partition,
          offset,
          key,
          value}).

-type message() :: #message{}.

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

% @hidden
start_link(Module, Args, GroupID, Topic, Partition) ->
  gen_server:start_link(?MODULE, [Module, Args, GroupID, Topic, Partition], []).

% @hidden
init([SubscriberModule, SubscriberArgs, GroupID, Topic, Partition]) ->
  erlang:process_flag(trap_exit, true),
  case erlang:apply(SubscriberModule, init, [GroupID, Topic, Partition, SubscriberArgs]) of
    {ok, SubscriberState} ->
      CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
      kafe_consumer_store:insert(GroupID, {subscriber_pid, CommitStoreKey}, self()),
      {ok, #state{
              group_id = GroupID,
              topic = Topic,
              partition = Partition,
              commit_store_key = CommitStoreKey,
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
      {ok, {error, Reason}, State#state{subscriber_state = NewSubscriberState}};
    {ok, NewSubscriberState} ->
      {reply, ok, State#state{subscriber_state = NewSubscriberState}}
  end;
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, #state{group_id = GroupID, commit_store_key = CommitStoreKey}) ->
  kafe_consumer_store:delete(GroupID, {subscriber_pid, CommitStoreKey}),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
