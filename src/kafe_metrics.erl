% @hidden
-module(kafe_metrics).
-compile([{parse_transform, lager_transform}]).

-export([
         init_consumer/1
         , delete_consumer/1
         , consumer_messages/2

         , init_consumer_partition/3
         , delete_consumer_partition/3
         , consumer_partition_messages/4
         , consumer_partition_duration/4
         , consumer_partition_pending_commits/4

         , ensure_metrics_mod_started/0
        ]).

ensure_metrics_mod_started() ->
  ensure_metrics_mod_started(metrics_backend()).
ensure_metrics_mod_started(metrics_folsom) ->
  application:ensure_all_started(folsom);
ensure_metrics_mod_started(metrics_exometer) ->
  application:ensure_all_started(exometer);
ensure_metrics_mod_started(metrics_grapherl) ->
  application:ensure_all_started(grapherl);
ensure_metrics_mod_started(_) ->
  ok.

init_consumer(Consumer) ->
  ensure_metrics_mod_started(),
  metrics_new(gauge, consumer_metric(Consumer, <<"messages.fetch">>)),
  metrics_new(counter, consumer_metric(Consumer, <<"messages">>)).

delete_consumer(Consumer) ->
  metrics_delete(consumer_metric(Consumer, <<"messages.fetch">>)),
  metrics_delete(consumer_metric(Consumer, <<"messages">>)).

consumer_messages(Consumer, NbMessages) ->
  metrics_update(consumer_metric(Consumer, <<"messages.fetch">>), NbMessages),
  metrics_update(consumer_metric(Consumer, <<"messages">>), {c, NbMessages}).

consumer_metric(Consumer, Ext) ->
  Prefix = case doteki:get_as_binary([metrics, metrics_prefix], <<>>) of
    <<>> -> <<>>;
    Other -> <<Other/binary, ".">>
  end,
  bucs:to_string(<<Prefix/binary,
                   "kafe_consumer.",
                   (bucs:to_binary(Consumer))/binary,
                   ".", Ext/binary>>).

init_consumer_partition(Consumer, Topic, Partition) ->
  ensure_metrics_mod_started(),
  metrics_new(gauge, consumer_partition_metric(Consumer, Topic, Partition, <<"messages.fetch">>)),
  metrics_new(gauge, consumer_partition_metric(Consumer, Topic, Partition, <<"duration.fetch">>)),
  metrics_new(counter, consumer_partition_metric(Consumer, Topic, Partition, <<"messages">>)),
  metrics_new(gauge, consumer_partition_metric(Consumer, Topic, Partition, <<"pending_commits">>)).

delete_consumer_partition(Consumer, Topic, Partition) ->
  metrics_delete(consumer_partition_metric(Consumer, Topic, Partition, <<"messages.fetch">>)),
  metrics_delete(consumer_partition_metric(Consumer, Topic, Partition, <<"duration.fetch">>)),
  metrics_delete(consumer_partition_metric(Consumer, Topic, Partition, <<"messages">>)),
  metrics_delete(consumer_partition_metric(Consumer, Topic, Partition, <<"pending_commits">>)).

consumer_partition_messages(Consumer, Topic, Partition, NbMessages) ->
  metrics_update(consumer_partition_metric(Consumer, Topic, Partition, <<"messages.fetch">>),
                 NbMessages),
  metrics_update(consumer_partition_metric(Consumer, Topic, Partition, <<"messages">>),
                 {c, NbMessages}).

consumer_partition_duration(Consumer, Topic, Partition, Duration) ->
  metrics_update(consumer_partition_metric(Consumer, Topic, Partition, <<"duration.fetch">>),
                 Duration).

consumer_partition_pending_commits(Consumer, Topic, Partition, NbCommits) ->
  metrics_update(consumer_partition_metric(Consumer, Topic, Partition, <<"pending_commits">>),
                 NbCommits).

consumer_partition_metric(Consumer, Topic, Partition, Ext) ->
  Prefix = case doteki:get_as_binary([metrics, metrics_prefix], <<>>) of
    <<>> -> <<>>;
    Other -> <<Other/binary, ".">>
  end,
  bucs:to_string(<<Prefix/binary,
                   "kafe_consumer.",
                   (bucs:to_binary(Consumer))/binary,
                   ".", (bucs:to_binary(Topic))/binary,
                   ".", (bucs:to_binary(Partition))/binary,
                   ".", Ext/binary>>).

-define(ENABLE_METRICS(X), case doteki:get_as_atom([kafe, enable_metrics], false) of
                             true -> X;
                             _ -> lager:debug("Metrics disabled!")
                           end).

metrics_new(Type, Metric) ->
  ?ENABLE_METRICS(metrics:new(Type, Metric)).

metrics_delete(Metric) ->
  ?ENABLE_METRICS(metrics:delete(Metric)).

metrics_update(Metric, Value) ->
  ?ENABLE_METRICS(metrics:update(Metric, Value)).

metrics_backend() ->
  ?ENABLE_METRICS(metrics:backend()).

