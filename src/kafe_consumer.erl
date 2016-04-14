% @author Grégoire Lejeune <gregoire.lejeune@botsunit.com>
% @copyright 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit
% @since 2014
% @doc
% A Kafka client for Erlang
%
% To create a consumer, use this behaviour :
%
% <pre>
% -module(my_consumer).
% -behaviour(kafe_consumer).
%
% -export([init/1, consume/3]).
%
% init(Args) ->
%   {ok, Args}.
%
% consume(Offset, Key, Value) ->
%   % Do something with Offset/Key/Value
%   ok.
% </pre>
%
% Then start a new consumer :
%
% <pre>
% ...
% kafe:start(),
% ...
% kafe:start_consumer(my_group, my_consumer, Options),
% ...
% </pre>
%
% When you are done with your consumer, stop it :
%
% <pre>
% ...
% kafe:stop_consumer(my_group),
% ...
% </pre>
% @end
-module(kafe_consumer).
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
          client_id = undefined,
          leader_id = undefined
         }).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupId, Options) ->
  gen_server:start_link({global, GroupId}, ?MODULE, [GroupId, Options], []).

%% gen_server.

% @hidden
init([GroupId, _Options]) ->
  _ = erlang:process_flag(trap_exit, true),
  lager:info("Start consumer ~p", [GroupId]),
  {ok, #state{
          group_id = bucs:to_binary(GroupId)
         }}.

% @hidden
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
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

