% @hidden
-module(kafe_consumer_tp_group_sup).
-behaviour(supervisor).

-export([start_link/11]).
-export([init/1]).

start_link(Topic, Partition, FetchInterval,
           GroupID, Autocommit, FromBeginning,
           MinBytes, MaxBytes, MaxWaitTime,
           Callback, Processing) ->
  supervisor:start_link(?MODULE, [Topic, Partition, FetchInterval,
                                  GroupID, Autocommit, FromBeginning,
                                  MinBytes, MaxBytes, MaxWaitTime,
                                  Callback, Processing]).

init([Topic, Partition, FetchInterval,
      GroupID, Autocommit, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      Callback, Processing]) when is_function(Callback) ->
  {ok, {
    #{strategy => one_for_all,
      intensity => 1,
      period => 5},
    [
      #{id => kafe_consumer_fetcher,
        start => {kafe_consumer_fetcher, start_link, [Topic, Partition, FetchInterval,
                                                      GroupID, Autocommit, FromBeginning,
                                                      MinBytes, MaxBytes, MaxWaitTime,
                                                      Callback, Processing]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_commiter,
        start => {kafe_consumer_commiter, start_link, [Topic, Partition, GroupID]},
        type => worker,
        shutdown => 5000}
    ]
  }};
init([Topic, Partition, FetchInterval,
      GroupID, Autocommit, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      Callback, Processing]) when is_atom(Callback);
                                  is_tuple(Callback)->
  {ok, {
    #{strategy => one_for_all,
      intensity => 1,
      period => 5},
    [
      #{id => kafe_consumer_fetcher,
        start => {kafe_consumer_fetcher, start_link, [Topic, Partition, FetchInterval,
                                                      GroupID, Autocommit, FromBeginning,
                                                      MinBytes, MaxBytes, MaxWaitTime,
                                                      Callback, Processing]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_commiter,
        start => {kafe_consumer_commiter, start_link, [Topic, Partition, GroupID]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_subscriber,
        start => {kafe_consumer_subscriber, start_link, [Callback, GroupID, Topic, Partition]},
        type => worker,
        shutdown => 5000}
    ]
  }}.
