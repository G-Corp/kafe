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
% -export([consume/3]).
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
% kafe:start_consumer(my_group, fun my_consumer:consume/3, Options),
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
-behaviour(supervisor).

-export([
         start/3,
         stop/1,
         describe/1
        ]).

-export([start_link/2]).
-export([init/1]).

% @equiv kafe:start_consumer(GroupId, Callback, Options)
start(GroupId, Callback, Options) ->
  kafe:start_consumer(GroupId, Callback, Options).

% @equiv kafe:stop_consumer(GroupId)
stop(GroupId) ->
  kafe:stop_consumer(GroupId).

% @doc
% Return consumer group descrition
% @end
-spec describe(atom()) -> {ok, kafe:describe_group()} | {error, term()}.
describe(GroupId) ->
  kafe_consumer_sup:call_srv(GroupId, describe).

% @hidden
start_link(GroupId, Options) ->
  supervisor:start_link({global, GroupId}, ?MODULE, [GroupId, Options]).

% @hidden
init([GroupId, Options]) ->
  {ok, {
     #{strategy => one_for_one,
       intensity => 1,
       period => 5},
     [#{id => kafe_consumer_fsm,
        start => {kafe_consumer_fsm, start_link, [GroupId, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_fsm]},
      #{id => kafe_consumer_srv,
        start => {kafe_consumer_srv, start_link, [GroupId, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_srv]}]
    }}.

