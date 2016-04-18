% @hidden
-module(kafe_consumer_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

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
          options = options
         }).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupId, Options) ->
  gen_server:start_link(?MODULE, [GroupId, Options], []).

%% gen_server.

% @hidden
init([GroupId, Options]) ->
  _ = erlang:process_flag(trap_exit, true),
  lager:info("Start consumer server ~p", [GroupId]),
  {ok, #state{
          group_id = bucs:to_binary(GroupId),
          options = Options
         }}.

% @hidden
handle_call(describe, _From, #state{group_id = GroupId} = State) ->
  {reply, kafe:describe_group(GroupId), State};
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

