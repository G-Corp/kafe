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

start_child(GroupID, Options) ->
  case supervisor:start_child(?MODULE, [GroupID, Options]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

stop_child(GroupID) when is_binary(GroupID) ->
  stop_child(bucs:to_atom(GroupID));
stop_child(GroupID) when is_atom(GroupID) ->
  case global:whereis_name(GroupID) of
    undefined -> undefined;
    Pid -> stop_child(Pid)
  end;
stop_child(GroupID) when is_pid(GroupID) ->
  supervisor:terminate_child(?MODULE, GroupID).

call_srv(GroupID, Request) when is_binary(GroupID) ->
  call_srv(bucs:to_atom(GroupID), Request);
call_srv(GroupID, Request) when is_atom(GroupID) ->
  case global:whereis_name(GroupID) of
    undefined -> undefined;
    Pid -> call_srv(Pid, Request)
  end;
call_srv(GroupID, Request) when is_pid(GroupID) ->
  case lists:keyfind(kafe_consumer_srv, 1, supervisor:which_children(GroupID)) of
    {kafe_consumer_srv, SrvPid, worker, [kafe_consumer_srv]} ->
      gen_server:call(SrvPid, Request);
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

