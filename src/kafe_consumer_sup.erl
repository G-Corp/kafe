% @hidden
-module(kafe_consumer_sup).

-behaviour(supervisor).

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
  case supervisor:start_child(?MODULE, [GroupID, Options]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

consumer_pid(GroupID) when is_binary(GroupID) ->
  consumer_pid(bucs:to_atom(GroupID));
consumer_pid(GroupID) when is_atom(GroupID) ->
  global:whereis_name(GroupID);
consumer_pid(GroupID) when is_pid(GroupID) ->
  GroupID.

server_pid(GroupID) ->
  case consumer_pid(GroupID) of
    undefined -> undefined;
    ConsumerPID ->
      case lists:keyfind(kafe_consumer_srv, 1, supervisor:which_children(ConsumerPID)) of
        {kafe_consumer_srv, SrvPID, worker, [kafe_consumer_srv]} ->
          SrvPID;
        false ->
          undefined
      end
  end.

stop_child(GroupID) ->
  case consumer_pid(GroupID) of
    undefined -> undefined;
    PID -> supervisor:terminate_child(?MODULE, PID)
  end.

call_srv(GroupID, Request) ->
  case server_pid(GroupID) of
    undefined -> {error, server_not_found};
    PID -> gen_server:call(PID, Request)
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

