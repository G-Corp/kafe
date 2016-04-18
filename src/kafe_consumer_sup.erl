% @hidden
-module(kafe_consumer_sup).

-behaviour(supervisor).

-export([
         start_link/0
         , start_child/2
         , stop_child/1
         , call_srv/2
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

call_srv(GroupId, Data) when is_atom(GroupId) ->
  case global:whereis_name(GroupId) of
    undefined -> undefined;
    Pid -> call_srv(Pid, Data)
  end;
call_srv(GroupId, Data) when is_pid(GroupId) ->
  case lists:keyfind(kafe_consumer_srv, 1, supervisor:which_children(GroupId)) of
    {kafe_consumer_srv, SrvPid, worker, [kafe_consumer_srv]} ->
      gen_server:call(SrvPid, Data);
    false ->
      undefined
  end.


init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => kafe_consumer,
        start => {kafe_consumer, start_link, []},
        type => supervisor,
        shutdown => 5000}]
    }}.

