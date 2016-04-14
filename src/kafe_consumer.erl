-module(kafe_consumer).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API.
-export([start_link/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
          group_id = undefined,
          client_id = undefined,
          leader_id = undefined
         }).

%% API.

-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupId, Options) ->
  gen_server:start_link({global, GroupId}, ?MODULE, [GroupId, Options], []).

%% gen_server.

init([GroupId, _Options]) ->
  _ = erlang:process_flag(trap_exit, true),
  lager:info("Start consumer ~p", [GroupId]),
  {ok, #state{
          group_id = bucs:to_binary(GroupId)
         }}.

handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

