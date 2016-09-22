% @hidden
-module(kafe_consumer_sup).
-behaviour(supervisor).

-include("../include/kafe.hrl").

-export([
         start_link/0
         , start_child/2
         , stop_child/1
         , server_pid/1
         , call_srv/2
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(GroupID, Options) ->
  case kafe_cst:lookup_srvpid(GroupID) of
    {ok, PID} ->
      {ok, PID};
    _ ->
      case supervisor:start_child(?MODULE, [GroupID, Options]) of
        {ok, Child, _} -> {ok, Child};
        Other -> Other
      end
  end.

stop_child(GroupPID) when is_pid(GroupPID) ->
  supervisor:terminate_child(?MODULE, GroupPID);
stop_child(GroupID) ->
  case kafe_cst:lookup_suppid(GroupID) of
    {ok, PID} ->
      stop_child(PID);
    _ ->
      {error, detached}
  end.

call_srv(GroupPID, Request) when is_pid(GroupPID) ->
  gen_server:call(GroupPID, Request);
call_srv(GroupID, Request) ->
  case kafe_cst:lookup_srvpid(GroupID) of
    {ok, PID} ->
      call_srv(PID, Request);
    _ ->
      {error, server_not_found}
  end.

server_pid(GroupID) ->
  kafe_cst:lookup_srvpid(GroupID).

init([]) ->
  kafe_cst:new(),
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => kafe_consumer,
        start => {kafe_consumer, start_link, []},
        type => supervisor,
        shutdown => 5000}]
    }}.

