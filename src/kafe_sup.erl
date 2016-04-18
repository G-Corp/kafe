% @hidden
-module(kafe_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(CHILD(I, Type, Shutdown), {I, {I, start_link, []}, permanent, Shutdown, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, { {one_for_one, 5, 10}, [
                                  ?CHILD(kafe_client_sup, supervisor, infinity),
                                  ?CHILD(kafe_conn_sup, supervisor, infinity),
                                  ?CHILD(kafe_consumer_sup, supervisor, infinity),
                                  ?CHILD(kafe, worker, 5000)
                                 ]} }.

