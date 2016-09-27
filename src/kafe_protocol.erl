% @hidden
-module(kafe_protocol).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/1,
         run/2,
         request/3,
         request/4,
         response/2,
         encode_string/1,
         encode_bytes/1,
         encode_array/1
        ]).

run(Request) ->
  case kafe:first_broker() of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end.

run(BrokerPID, Request) when is_pid(BrokerPID) ->
  try
    Response = gen_server:call(BrokerPID, Request, ?TIMEOUT),
    _ = kafe:release_broker(BrokerPID),
    Response
  catch
    Type:Error ->
      lager:info("Request error : ~p:~p", [Type, Error]),
      {error, Error}
  end;
run(BrokerName, Request) when is_list(BrokerName) ->
  case kafe:broker_by_name(BrokerName) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end;
run(BrokerID, Request) when is_atom(BrokerID) ->
  case kafe:broker_by_id(BrokerID) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end;
run({host_and_port, Host, Port}, Request) ->
  case kafe:broker_by_host_and_port(Host, Port) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      run(BrokerPID, Request)
  end;
run({topic_and_partition, Topic, Partition}, Request) ->
  case kafe:broker_id_by_topic_and_partition(Topic, Partition) of
    undefined ->
      {error, no_broker_found};
    BrokerID ->
      run(BrokerID, Request)
  end.

request(ApiKey, RequestMessage,
        #{api_version := ApiVersion} = State) ->
  request(ApiKey, RequestMessage, State, ApiVersion).
request(ApiKey, RequestMessage,
        #{correlation_id := CorrelationId,
          client_id := ClientId} = State,
       ApiVersion) ->
  #{packet => encode_bytes(<<
                             ApiKey:16/signed,
                             ApiVersion:16/signed,
                             CorrelationId:32/signed,
                             (encode_string(ClientId))/binary,
                             RequestMessage/binary
                           >>),
    state => maps:update(correlation_id, CorrelationId + 1, State),
    api_version => ApiVersion}.

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

response(
  <<Size:32/signed, Packet:Size/bytes>>,
  #{requests := Requests, sndbuf := SndBuf, recbuf := RecBuf, buffer := Buffer} = State
 ) ->
  <<CorrelationId:32/signed, Remainder/bytes>> = Packet,
  case orddict:find(CorrelationId, Requests) of
    {ok, #{from := From, handler := {ResponseHandler, ResponseHandlerParams}, socket := Socket, api_version := ApiVersion}} ->
      _ = gen_server:reply(From, erlang:apply(ResponseHandler, [Remainder, ApiVersion|ResponseHandlerParams])),
      case inet:setopts(Socket, [{active, once}, {sndbuf, SndBuf}, {recbuf, RecBuf}, {buffer, Buffer}]) of
        ok ->
          {noreply, maps:update(requests, orddict:erase(CorrelationId, Requests), State)};
        {error, _} = Reason ->
          {stop, Reason, State}
      end;
    error ->
      {noreply, State} %;
  end.

