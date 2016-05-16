% @hidden
-module(kafe_consumer_fetcher_sup).

-behaviour(supervisor).

-export([
         start_link/0
         , stop_child/1
         , start_child/13
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_child(Pid) when is_pid(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

start_child(Topic, Partition, Srv, FetchInterval,
            GroupID, GenerationID, MemberID,
            FetchSize, Autocommit,
            MinBytes, MaxBytes, MaxWaitTime, Callback) ->
  case supervisor:start_child(?MODULE, [Topic, Partition, Srv, FetchInterval,
                                        GroupID, GenerationID, MemberID,
                                        FetchSize, Autocommit,
                                        MinBytes, MaxBytes, MaxWaitTime, Callback]) of
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

