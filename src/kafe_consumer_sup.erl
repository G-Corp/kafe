% @hidden
-module(kafe_consumer_sup).

-behaviour(supervisor).

-export([
         start_link/0
         , start_child/2
         , stop_child/1
        ]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(GroupId, Options) ->
  case supervisor:start_child(?MODULE, [GroupId, Options]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

stop_child(GroupId) when is_atom(GroupId) ->
  case global:whereis_name(GroupId) of
    undefined -> undefined;
    Pid -> stop_child(Pid)
  end;
stop_child(GroupId) when is_pid(GroupId) ->
  supervisor:terminate_child(?MODULE, GroupId).

init([]) ->
  SupFlags = #{strategy => simple_one_for_one,
               intensity => 0,
               period => 1},
  ChildSpecs = [#{id => kafe_consumer,
                  start => {kafe_consumer, start_link, []},
                  shutdown => 2000}],
  {ok, {SupFlags, ChildSpecs}}.

