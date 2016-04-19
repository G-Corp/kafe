% @hidden
-module(poolgirl_sup).
-behaviour(supervisor).

-export([
         start_link/0,
         add_pool/2,
         remove_pool/1
        ]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_pool(Name, {Module, Function, _}) ->
  case supervisor:start_child(
         ?MODULE,
         #{id => Name,
           start => {poolgirl_worker_sup, start_link, [Name, Module, Function]},
           restart => permanent,
           shutdown => 5000,
           type => supervisor,
           modules => [poolgirl_worker_sup]}) of
    {ok, PID} ->
      {ok, PID};
    {ok, PID, _Info} ->
      {ok, PID};
    {error, {already_started, _}} ->
      {error, already_started};
    E ->
      E
  end.

remove_pool(PID) ->
  case supervisor:terminate_child(?MODULE, PID) of
    ok ->
      supervisor:delete_child(?MODULE, PID);
    E -> E
  end.

init([]) ->
  {ok, {
     #{strategy => one_for_all,
       intensity => 5,
       period => 10},
     [#{id => poolgirl,
        start => {poolgirl, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [poolgirl]}]
    }}.
