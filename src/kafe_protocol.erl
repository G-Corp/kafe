-module(kafe_protocol).
-export([
         request/3,
         encode_string/1,
         encode_bytes/1,
         encode_array/1
        ]).

request(ApiKey, RequestMessage, 
        #{api_version := ApiVersion, 
          correlation_id := CorrelationId, 
          client_id := ClientId} = State) ->
  #{packet => encode_bytes(<<
                      ApiKey:16/signed, 
                      ApiVersion:16/signed, 
                      CorrelationId:32/signed, 
                      (encode_string(ClientId))/binary,
                      RequestMessage/binary
                    >>),
    state => maps:update(correlation_id, CorrelationId + 1, State)}.

encode_string(undefined) ->
    <<-1:16/signed>>;
encode_string(Data) when is_binary(Data) ->
  <<(byte_size(Data)):16/signed, Data/binary>>.

encode_bytes(undefined) ->
    <<-1:32/signed>>;
encode_bytes(Data) ->
  <<(byte_size(Data)):32/signed, Data/binary>>.

encode_array(List) ->
    Len = length(List),
    Payload = << <<B/binary>> || B <- List>>,
    <<Len:32/signed, Payload/binary>>.

