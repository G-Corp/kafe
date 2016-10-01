% @hidden
-module(kafe_consumer_fetcher_sup).
-compile([{parse_transform, lager_transform}]).

-behaviour(supervisor).

-export([
         start_link/0
         , stop_child/1
         , start_child/10
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_child(Pid) when is_pid(Pid) ->
  case erlang:is_process_alive(Pid) of
    true ->
      try
        supervisor:terminate_child(?MODULE, Pid)
      catch
        C:E ->
          lager:error("Can't terminate kafe_consumer_fetcher #~p: ~p:~p", [Pid, C, E]),
          {error, E}
      end;
    false ->
      {error, not_found}
  end.


start_child(Topic, Partition, FetchInterval,
            GroupID, Autocommit, MinBytes, MaxBytes,
            MaxWaitTime, Callback, Processing) ->
  case supervisor:start_child(?MODULE, [Topic, Partition, FetchInterval,
                                        GroupID, Autocommit, MinBytes, MaxBytes,
                                        MaxWaitTime, Callback, Processing]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => kafe_consumer_fetcher,
        % start => {kafe_consumer_fetcher, start_link, []},
        start => {kafe_consumer_fetcher_commiter_sup, start_link, []},
        type => supervisor,
        shutdown => 5000}]
    }}.

