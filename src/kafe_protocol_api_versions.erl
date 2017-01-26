-module(kafe_protocol_api_versions).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/0,
         request/1,
         response/3 % TODO /2
        ]).

run() ->
  kafe_protocol:run(
    ?API_VERSIONS_REQUEST,
    fun ?MODULE:request/1,
    fun ?MODULE:response/3).

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
         >>, _ApiVersion, _State) -> % TODO remove _ApiVersion
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

