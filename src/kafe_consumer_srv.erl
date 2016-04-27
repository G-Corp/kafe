% @hidden
-module(kafe_consumer_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

%% API.
-export([start_link/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
          group_id,
          generation_id = -1,
          member_id = <<>>,
          topics = []
         }).

%% API.

% @hidden
-spec start_link(atom()) -> {ok, pid()}.
start_link(GroupId) ->
  gen_server:start_link(?MODULE, GroupId, []).

%% gen_server.

% @hidden
init(GroupId) ->
  _ = erlang:process_flag(trap_exit, true),
  {ok, #state{
          group_id = bucs:to_binary(GroupId)
         }}.

% @hidden
handle_call(describe, _From, #state{group_id = GroupId} = State) ->
  {reply, kafe:describe_group(GroupId), State};
handle_call(member_id, _From, #state{member_id = MemberId} = State) ->
  {reply, MemberId, State};
handle_call({member_id, MemberId}, _From, State) ->
  {reply, ok, State#state{member_id = MemberId}};
handle_call(generation_id, _From, #state{generation_id = GenerationId} = State) ->
  {reply, GenerationId, State};
handle_call({generation_id, GenerationId}, _From, State) ->
  {reply, ok, State#state{generation_id = GenerationId}};
handle_call(topics, _From, #state{topics = Topics} = State) ->
  {reply, Topics, State};
handle_call({topics, Topics}, _From, State) ->
  {reply, ok, State#state{topics = Topics}};
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  lager:info("Terminate server !!!"),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

