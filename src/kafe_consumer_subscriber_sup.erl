% @hidden
-module(kafe_consumer_subscriber_sup).
-behaviour(supervisor).

-export([
         start_link/0
         , stop_child/1
         , start_child/5
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_child(Pid) when is_pid(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

start_child(Module, Args, GroupID, Topic, Partition) ->
  case supervisor:start_child(?MODULE, [Module, Args, GroupID, Topic, Partition]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [
      #{id => kafe_consumer_subscriber,
        start => {kafe_consumer_subscriber, start_link, []},
        type => worker,
        shutdown => 5000}
     ]
    }}.
