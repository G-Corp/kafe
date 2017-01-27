% @hidden
-module(kafe_protocol).
-compile([{parse_transform, lager_transform}]).

-include("../include/kafe.hrl").

-export([
         run/3,
         run/4,
         request/2,
         response/2,
         encode_string/1,
         encode_bytes/1,
         encode_array/1
        ]).

run(ApiKey, RequestFun, ResponseFun) when is_integer(ApiKey) ->
  run(ApiKey, RequestFun, ResponseFun, #{}).

run(ApiKey, RequestFun, ResponseFun, State) when is_integer(ApiKey),
                                                 is_function(RequestFun),
                                                 is_map(State)->
  run(ApiKey, {RequestFun, []}, ResponseFun, State);
run(ApiKey, RequestFun, ResponseFun, State) when is_integer(ApiKey),
                                                 (is_function(ResponseFun) orelse ResponseFun == undefined),
                                                 is_map(State)->
  run(ApiKey, RequestFun, {ResponseFun, []}, State);
run(ApiKey, {RequestFun, RequestParams}, {ResponseFun, ResponseParams}, State) when is_integer(ApiKey),
                                                                                    is_function(RequestFun),
                                                                                    is_list(RequestParams),
                                                                                    (is_function(ResponseFun) orelse ResponseFun == undefined),
                                                                                    is_list(ResponseParams),
                                                                                    is_map(State) ->
  ApiVersion = case maps:get(api_version, State, undefined) of
                 undefined ->
                   case ApiKey of
                     ?API_VERSIONS_REQUEST -> 0;
                     _ -> kafe:api_version(ApiKey)
                   end;
                 V -> V
               end,
  Broker = maps:get(broker, State, first_broker),
  do_run(Broker,
         {call,
          {RequestFun, RequestParams},
          {ResponseFun, ResponseParams},
          State#{api_key => ApiKey,
                 api_version => ApiVersion}}).

request(RequestMessage, #{api_key := ApiKey,
                          api_version := ApiVersion,
                          correlation_id := CorrelationId,
                          client_id := ClientId} = State) ->
  #{packet => <<
                ApiKey:16/signed,
                ApiVersion:16/signed,
                CorrelationId:32/signed,
                (encode_string(ClientId))/binary,
                RequestMessage/binary
              >>,
    state => maps:update(correlation_id, CorrelationId + 1, State),
    api_version => ApiVersion}.

% PRIVATE

do_run(first_broker, Request) ->
  case kafe_brokers:first_broker(false) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run(BrokerPID, Request) when is_pid(BrokerPID) ->
  case erlang:is_process_alive(BrokerPID) of
    true ->
      try
        Response = gen_server:call(BrokerPID, Request, ?TIMEOUT),
        _ = kafe_brokers:release_broker(BrokerPID),
        Response
      catch
        Type:Error ->
          _ = kafe_brokers:release_broker(BrokerPID),
          lager:error("Request error: ~p:~p", [Type, Error]),
          {error, Error}
      end;
    false ->
      _ = kafe_brokers:release_broker(BrokerPID),
      {error, broker_not_available}
  end;
do_run(BrokerName, Request) when is_list(BrokerName) ->
  case kafe_brokers:broker_by_name(BrokerName) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run(BrokerID, Request) when is_atom(BrokerID) ->
  case kafe_brokers:broker_by_id(BrokerID) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run({host_and_port, Host, Port}, Request) ->
  case kafe_brokers:broker_by_host_and_port(Host, Port) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run({topic_and_partition, Topic, Partition}, Request) ->
  case kafe_brokers:broker_by_topic_and_partition(Topic, Partition) of
    undefined ->
      {error, no_broker_found};
    BrokerPID ->
      do_run(BrokerPID, Request)
  end;
do_run({coordinator, GroupId}, Request) ->
  case kafe:group_coordinator(bucs:to_binary(GroupId)) of
    {ok, #{coordinator_host := Host,
           coordinator_port := Port,
           error_code := none}} ->
      case do_run({host_and_port, Host, Port}, Request) of
        {ok, #{error_code := not_coordinator_for_group}} ->
          retry_with_coordinator(GroupId, Request);
        {ok, [#{error_code := not_coordinator_for_group}]} ->
          retry_with_coordinator(GroupId, Request);
        Other ->
          Other
      end;
    _ ->
      {error, no_broker_found}
  end.

retry_with_coordinator(GroupId, Request) ->
  case kafe_protocol_group_coordinator:run(GroupId, force) of
    {ok, #{error_code := none}} ->
      do_run({coordinator, GroupId}, Request);
    _ ->
      {error, no_broker_found}
  end.

% PRIVATE

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

response(<<CorrelationId:32/signed, Remainder/bytes>>, #{requests := Requests} = State) ->
  case orddict:find(CorrelationId, Requests) of
    {ok, #{from := From, handler := {ResponseHandler, ResponseHandlerParams}, api_version := ApiVersion}} -> % TODO : remove api_version
      _ = gen_server:reply(From, erlang:apply(ResponseHandler, [Remainder, ApiVersion|ResponseHandlerParams])), % TODO : remove api_version
      {ok, maps:update(requests, orddict:erase(CorrelationId, Requests), State)};
    error ->
      {error, request_not_found} %;
  end.

