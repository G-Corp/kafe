-module(kafe_protocol_api_versions).

-include("../include/kafe.hrl").

-export([
         run/0,
         request/1,
         response/2
        ]).

run() ->
  kafe_protocol:run({call,
                     fun ?MODULE:request/1, [],
                     fun ?MODULE:response/2}).

request(State) ->
  kafe_protocol:request(
    ?API_VERSIONS_REQUEST,
    <<>>,
    State,
    ?V0).

response(<<
           ErrorCode:16/signed,
           ApiVersionsLen:32/signed,
           ApiVersions/binary
         >>, _ApiVersion) ->
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

