% @hidden
-module(poolgirl_worker_sup).

-behaviour(supervisor).

-export([start_link/3]).
-export([init/1]).

start_link(Name, Module, Function) ->
  supervisor:start_link({local, Name}, ?MODULE, [Module, Function]).

init([Module, Function]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => Module,
        start => {Module, Function, []},
        restart => temporary,
        type => worker,
        shutdown => 5000}]
    }}.

