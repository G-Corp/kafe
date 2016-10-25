% @hidden
-module(kafe_consumer_tp_group_sup).
-behaviour(supervisor).

-export([start_link/10]).
-export([init/1]).

start_link(Topic, Partition, FetchInterval,
           GroupID, Commit, FromBeginning,
           MinBytes, MaxBytes, MaxWaitTime,
           Callback) ->
  supervisor:start_link(?MODULE, [Topic, Partition, FetchInterval,
                                  GroupID, Commit, FromBeginning,
                                  MinBytes, MaxBytes, MaxWaitTime,
                                  Callback]).

init([Topic, Partition, FetchInterval,
      GroupID, Commit, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      Callback]) when is_function(Callback) ->
  {ok, {
    #{strategy => one_for_all,
      intensity => 1,
      period => 5},
    [
      #{id => kafe_consumer_committer,
        start => {kafe_consumer_committer, start_link, [Topic, Partition, GroupID, Commit]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_fetcher,
        start => {kafe_consumer_fetcher, start_link, [Topic, Partition, FetchInterval,
                                                      GroupID, Commit, FromBeginning,
                                                      MinBytes, MaxBytes, MaxWaitTime,
                                                      Callback]},
        type => worker,
        shutdown => 5000}
    ]
  }};
init([Topic, Partition, FetchInterval,
      GroupID, Commit, FromBeginning,
      MinBytes, MaxBytes, MaxWaitTime,
      Callback]) when is_atom(Callback);
                      is_tuple(Callback)->
  {ok, {
    #{strategy => one_for_all,
      intensity => 1,
      period => 5},
    [
      #{id => kafe_consumer_committer,
        start => {kafe_consumer_committer, start_link, [Topic, Partition, GroupID, Commit]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_subscriber,
        start => {kafe_consumer_subscriber, start_link, [Callback, GroupID, Topic, Partition]},
        type => worker,
        shutdown => 5000},
      #{id => kafe_consumer_fetcher,
        start => {kafe_consumer_fetcher, start_link, [Topic, Partition, FetchInterval,
                                                      GroupID, Commit, FromBeginning,
                                                      MinBytes, MaxBytes, MaxWaitTime,
                                                      Callback]},
        type => worker,
        shutdown => 5000}
    ]
  }}.
