% @hidden
-module(kafe_client_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/2]).
-export([init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Addr, Port) ->
  IP = enet:ip_to_str(Addr),
  ClientID = kafe_utils:broker_id(IP, Port),
  case supervisor:start_child(?MODULE, 
                         {ClientID, 
                          {kafe_client, start_link, [ClientID, Addr, Port]},
                          permanent,
                          infinity,
                          worker,
                          [kafe_client]}) of
    {ok, _ChildPID} ->
      {ok, ClientID};
    {ok, _ChildPID, _Info} ->
      {ok, ClientID};
    E ->
      E
  end.

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.

