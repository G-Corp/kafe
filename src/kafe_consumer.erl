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
-behaviour(supervisor).

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

-export([start_link/2]).
-export([init/1]).

start_link(GroupId, Options) ->
  supervisor:start_link({global, GroupId}, ?MODULE, [GroupId, Options]).

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

