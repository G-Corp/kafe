% @hidden
-module(kafe_conn_sup).

-behaviour(supervisor).

-export([
         start_link/0
         , start_child/2
         , stop_child/1
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Addr, Port) ->
  case supervisor:start_child(?MODULE, [Addr, Port]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

stop_child(ConnPid) when is_pid(ConnPid) ->
  supervisor:terminate_child(?MODULE, ConnPid).

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => kafe_conn,
        start => {kafe_conn, start_link, []},
        type => supervisor,
        shutdown => 5000}]
    }}.

