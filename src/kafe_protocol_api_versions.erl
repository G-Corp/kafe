% @hidden
-module(kafe_protocol_api_versions).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").
-define(MAX_VERSION, 0).

-export([
         run/0,
         request/1,
         response/2
        ]).

run() ->
  case kafe_protocol:run(
    ?API_VERSIONS_REQUEST,
    ?MAX_VERSION,
    fun ?MODULE:request/1,
    fun ?MODULE:response/2) of
    {error, {timeout, _}} ->
      {ok, #{error_code => none,
             api_versions =>
             [
              #{api_key => ?PRODUCE_REQUEST,
                min_version => 0,
                max_version => 1},
              #{api_key => ?FETCH_REQUEST,
                min_version => 0,
                max_version => 1},
              #{api_key => ?OFFSET_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?METADATA_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?LEADER_AND_ISR_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?STOP_REPLICA_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?UPDATE_METADATA_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?CONTROLLED_SHUTDOWN_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?OFFSET_COMMIT_REQUEST,
                min_version => 2,
                max_version => 0},
              #{api_key => ?OFFSET_FETCH_REQUEST,
                min_version => 0,
                max_version => 1},
              #{api_key => ?GROUP_COORDINATOR_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?JOIN_GROUP_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?HEARTBEAT_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?LEAVE_GROUP_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?SYNC_GROUP_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?DESCRIBE_GROUPS_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?LIST_GROUPS_REQUEST,
                min_version => 0,
                max_version => 0},
              #{api_key => ?SASL_HANDSHAKE_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?API_VERSIONS_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?CREATE_TOPICS_REQUEST,
                min_version => -1,
                max_version => -1},
              #{api_key => ?DELETE_TOPICS_REQUEST,
                min_version => -1,
                max_version => -1}
             ]}};
    Result ->
      Result
  end.

% ApiVersions Request (Version: 0) =>
request(State) ->
  kafe_protocol:request(<<>>, State).

% ApiVersions Response (Version: 0) => error_code [api_versions]
%   error_code => INT16
%   api_versions => api_key min_version max_version
%     api_key => INT16
%     min_version => INT16
%     max_version => INT16
response(<<
           ErrorCode:16/signed,
           ApiVersionsLen:32/signed,
           ApiVersions/binary
         >>,
         _State) ->
  {ok, #{error_code => kafe_error:code(ErrorCode),
         api_versions => api_versions(ApiVersionsLen, ApiVersions, [])}}.

api_versions(0, _, Acc) ->
  Acc;
api_versions(N, <<
                  ApiKey:16/signed,
                  MinVersion:16/signed,
                  MaxVersion:16/signed,
                  Remainder/binary
                >>, Acc) ->
  api_versions(N - 1,
               Remainder,
               [#{api_key => ApiKey,
                  min_version => MinVersion,
                  max_version => MaxVersion}|Acc]).

