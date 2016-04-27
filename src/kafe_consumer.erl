% @author Grégoire Lejeune <gregoire.lejeune@botsunit.com>
% @copyright 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit
% @since 2014
% @doc
% A Kafka client for Erlang
%
% To create a consumer, create a function with 5 parameters :
%
% <pre>
% -module(my_consumer).
% -behaviour(kafe_consumer).
%
% -export([consume/5]).
%
% consume(Topic, Partition, Offset, Key, Value) ->
%   % Do something with Topic/Partition/Offset/Key/Value
%   ok.
% </pre>
%
% Then start a new consumer :
%
% <pre>
% ...
% kafe:start(),
% ...
% kafe:start_consumer(my_group, fun my_consumer:consume/5, Options),
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
         start/3
         , stop/1
         , describe/1
         , member_id/1
         , generation_id/1
         , topics/1
        ]).

-export([
         start_link/2
         , init/1
         , member_id/2
         , generation_id/2
         , topics/2
        ]).

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
member_id(GroupId, MemberId) ->
  kafe_consumer_sup:call_srv(GroupId, {member_id, MemberId}).

% @doc
% Return the <tt>member_id</tt> of the consumer
% @end
member_id(GroupId) ->
  kafe_consumer_sup:call_srv(GroupId, member_id).

% @hidden
generation_id(GroupId, MemberId) ->
  kafe_consumer_sup:call_srv(GroupId, {generation_id, MemberId}).

% @doc
% Return the <tt>generation_id</tt> of the consumer
% @end
generation_id(GroupId) ->
  kafe_consumer_sup:call_srv(GroupId, generation_id).

% @hidden
topics(GroupId, MemberId) ->
  kafe_consumer_sup:call_srv(GroupId, {topics, MemberId}).

% @doc
% Return the topics (and partitions) of the consumer
% @end
topics(GroupId) ->
  kafe_consumer_sup:call_srv(GroupId, topics).

% @hidden
start_link(GroupId, Options) ->
  supervisor:start_link({global, bucs:to_atom(GroupId)}, ?MODULE, [GroupId, Options]).

% @hidden
init([GroupId, Options]) ->
  {ok, {
     #{strategy => one_for_one,
       intensity => 1,
       period => 5},
     [
      #{id => kafe_consumer_srv,
        start => {kafe_consumer_srv, start_link, [GroupId, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_srv]},
      #{id => kafe_consumer_fsm,
        start => {kafe_consumer_fsm, start_link, [GroupId, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_fsm]}
     ]
    }}.

