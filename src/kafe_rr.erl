% @hidden
-module(kafe_rr).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% API.
-export([start_link/0]).
-export([next/1]).
-export([stop/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
  gen_server:call(?SERVER, stop).

next(Topic) when is_binary(Topic) ->
  gen_server:call(?SERVER, {next, Topic}).

%% gen_server.

init([]) ->
  {ok, #{}}.

handle_call({next, Topic}, _From, State) ->
  Partitions = case State of
                 #{Topic := P} -> P;
                 _ -> lists:reverse(kafe:partitions(Topic))
               end,
  case Partitions of
    [] ->
      {reply, 0, State};
    _ ->
      [Next|R] = lists:reverse(Partitions),
      {reply, Next, State#{Topic => [Next|lists:reverse(R)]}}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, shutdown_ok, State};
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

