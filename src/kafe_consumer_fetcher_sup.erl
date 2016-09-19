% @hidden
-module(kafe_consumer_fetcher_sup).

-behaviour(supervisor).

-export([
         start_link/0
         , stop_child/1
         , start_child/14
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_child(Pid) when is_pid(Pid) ->
  try
    supervisor:terminate_child(?MODULE, Pid)
  catch
    C:E ->
      lager:error("Can't terminate kafe_consumer_fetcher #~p: ~p:~p", [Pid, C, E]),
      {error, E}
  end.


start_child(Topic, Partition, Srv, FetchInterval,
            GroupID, GenerationID, MemberID,
            FetchSize, Autocommit,
            MinBytes, MaxBytes, MaxWaitTime,
            Callback, Processing) ->
  case supervisor:start_child(?MODULE, [Topic, Partition, Srv, FetchInterval,
                                        GroupID, GenerationID, MemberID,
                                        FetchSize, Autocommit,
                                        MinBytes, MaxBytes, MaxWaitTime,
                                        Callback, Processing]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [#{id => kafe_consumer_fetcher,
        start => {kafe_consumer_fetcher, start_link, []},
        type => supervisor,
        shutdown => 5000}]
    }}.

