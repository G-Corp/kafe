% @hidden
-module(kafe_error).

-export([code/1]).

code(0) ->
    no_error;
code(-1) ->
    unknown;
code(1) ->
    offset_out_of_range;
code(2) ->
    invalid_message;
code(3) ->
    unknown_topic_or_partition;
code(4) ->
    invalid_message_size;
code(5) ->
    leader_not_available;
code(6) ->
    not_leader_for_partition;
code(7) ->
    request_timed_out;
code(8) ->
    broker_not_available;
code(9) ->
    replica_not_available;
code(10) ->
    message_size_too_large;
code(11) ->
    stale_controller_epoch;
code(12) ->
    offset_metadata_too_large;
code(14) ->
    offsets_load_in_progress;
code(15) ->
    consumer_coordinator_not_available;
code(16) ->
    not_coordinator_for_consumer.

