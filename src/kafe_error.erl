% @hidden
-module(kafe_error).

-export([code/1
         , retry/1
         , message/1]).

-define(UNKNOW_ERROR,
        {unknown, -1, false, "The server experienced an unexpected error when processing the request"}).
-define(ERRORS,
        [
         ?UNKNOW_ERROR,
         {none, 0, false, ""},
         {offset_out_of_range, 1, false, "The requested offset is not within the range of offsets maintained by the server."},
         {corrupt_message, 2, true, "The message contents does not match the message CRC or the message is otherwise corrupt."},
         {unknown_topic_or_partition, 3, true, "This server does not host this topic-partition."},
         {leader_not_available, 5, true, "There is no leader for this topic-partition as we are in the middle of a leadership election."},
         {not_leader_for_partition, 6, true, "This server is not the leader for that topic-partition."},
         {request_timed_out, 7, true, "The request timed out."},
         {broker_not_available, 8, false, "The broker is not available."},
         {replica_not_available, 9, false, "The replica is not available for the requested topic-partition"},
         {message_too_large, 10, false, "The request included a message larger than the max message size the server will accept."},
         {stale_controller_epoch, 11, false, "The controller moved to another broker."},
         {offset_metadata_too_large, 12, false, "The metadata field of the offset request was too large."},
         {network_exception, 13, true, "The server disconnected before a response was received."},
         {group_load_in_progress, 14, true, "The coordinator is loading and hence can't process requests for this group."},
         {group_coordinator_not_available, 15, true, "The group coordinator is not available."},
         {not_coordinator_for_group, 16, true, "This is not the correct coordinator for this group."},
         {invalid_topic_exception, 17, false, "The request attempted to perform an operation on an invalid topic."},
         {record_list_too_large, 18, false, "The request included message batch larger than the configured segment size on the server."},
         {not_enough_replicas, 19, true, "Messages are rejected since there are fewer in-sync replicas than required."},
         {not_enough_replicas_after_append, 20, true, "Messages are written to the log, but to fewer in-sync replicas than required."},
         {invalid_required_acks, 21, false, "Produce request specified an invalid value for required acks."},
         {illegal_generation, 22, false, "Specified group generation id is not valid."},
         {inconsistent_group_protocol, 23, false, "The group member's supported protocols are incompatible with those of existing members."},
         {invalid_group_id, 24, false, "The configured groupId is invalid"},
         {unknown_member_id, 25, false, "The coordinator is not aware of this member."},
         {invalid_session_timeout, 26, false, "The session timeout is not within an acceptable range."},
         {rebalance_in_progress, 27, false, "The group is rebalancing, so a rejoin is needed."},
         {invalid_commit_offset_size, 28, false, "The committing offset data size is not valid "},
         {topic_authorization_failed, 29, false, "Topic authorization failed."},
         {group_authorization_failed, 30, false, "Group authorization failed."},
         {cluster_authorization_failed, 31, false, "Cluster authorization failed."},
         {invalid_timestamp, 32, false, "The timestamp of the message is out of acceptable range."},
         {unsupported_sasl_mechanism, 33, false, "The broker does not support the requested SASL mechanism."},
         {illegal_sasl_state, 34, false, "Request is not valid given the current SASL state."},
         {unsupported_version, 35, false, "The version of API is not supported."},
         {topic_already_exists, 36, false, "Topic with this name already exists."},
         {invalid_partitions, 37, false, "Number of partitions is invalid."},
         {invalid_replication_factor, 38, false, "Replication-factor is invalid."},
         {invalid_replica_assignment, 39, false, "Replica assignment is invalid."},
         {invalid_config, 40, false, "Configuration is invalid."},
         {not_controller, 41, true, "This is not the correct controller for this cluster."},
         {invalid_request, 42, false, "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. " ++
          "See the broker logs for more details."},
         {unsupported_for_message_format, 43, false, "The message format version on the broker does not support the request."}
        ]).

code(N) when is_integer(N) ->
  buclists:keyfind(N, 2, ?ERRORS, 1, ?UNKNOW_ERROR);
code(N) when is_atom(N) ->
  buclists:keyfind(N, 1, ?ERRORS, 1, ?UNKNOW_ERROR).

retry(N) when is_integer(N) ->
  buclists:keyfind(N, 2, ?ERRORS, 3, ?UNKNOW_ERROR);
retry(N) when is_atom(N) ->
  buclists:keyfind(N, 1, ?ERRORS, 3, ?UNKNOW_ERROR).

message(N) when is_integer(N) ->
  buclists:keyfind(N, 2, ?ERRORS, 4, ?UNKNOW_ERROR);
message(N) when is_atom(N) ->
  buclists:keyfind(N, 1, ?ERRORS, 4, ?UNKNOW_ERROR).

