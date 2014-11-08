-module(kafe).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([
         metadata/0,
         metadata/1,
         offset/2,
         produce/2,
         produce/3,
         fetch/2,
         fetch/3
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

metadata() ->
  gen_server:call(?SERVER, {metadata, []}).

metadata(Topics) when is_list(Topics) ->
  gen_server:call(?SERVER, {metadata, Topics}).

% kafe:offset(0, [<<"public">>]).
% kafe:offset(1, [{<<"public">>, [{0, -1, 65535}]}, <<"service">>]).
offset(Replica, Topics) ->
  gen_server:call(?SERVER, {offset, Replica, Topics}).

% Topic :: binary()
% Message :: binary() | {binary(), binary()}
% Options :: #{timeout => integer(), required_acks => integer(), partition => integer()}
produce(Topic, Message) ->
  produce(Topic, Message, #{}).
produce(Topic, Message, Options) ->
  gen_server:call(?SERVER, {produce, Topic, Message, Options}).

% Replica = integer()
% TopicName = binary()
% Options = #{Key => Value}
%   Key = partition | offset | max_bytes | min_bytes | max_wait_time
%   Value = term()
fetch(Replica, TopicName) ->
  fetch(Replica, TopicName, #{}).
fetch(Replica, TopicName, Options) ->
  gen_server:call(?SERVER, {fetch, Replica, TopicName, Options}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_) ->
  KafkaIP = case application:get_env(kafe, host) of
              {ok, IP} -> IP;
              _ -> ?DEFAULT_IP
            end,
  KafkaPort = case application:get_env(kafe, port) of
                {ok, Port} -> Port;
                _ -> ?DEFAULT_PORT
              end,
  ClientID = case application:get_env(kafe, client_id) of
               {ok, CID} -> eutils:to_binary(CID);
               _ -> ?DEFAULT_CLIENT_ID
             end,
  CorrelationID = case application:get_env(kafe, correlation_id) of
                    {ok, CorrID} -> CorrID;
                    _ -> ?DEFAULT_CORRELATION_ID
                  end,
  ApiVersion = case application:get_env(kafe, api_version) of
                 {ok, Version} -> Version;
                 _ -> ?DEFAULT_API_VERSION
               end,
  Offset = case application:get_env(kafe, offset) of
             {ok, Off} -> Off;
             _ -> ?DEFAULT_OFFSET
           end,
  case gen_tcp:connect(KafkaIP, KafkaPort, [{mode, binary}, {active, once}]) of
    {ok, Socket} ->
      {ok, #{host => KafkaIP, 
             port => KafkaPort, 
             socket => Socket,
             api_version => ApiVersion,
             correlation_id => CorrelationID,
             client_id => ClientID,
             requests => orddict:new(),
             parts => <<>>,
             offset => Offset
            }};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_call({metadata, Topics}, From, State) ->
  send_request(kafe_protocol_metadata:request(Topics, State), 
               From, 
               fun kafe_protocol_metadata:response/1, 
               State);
handle_call({offset, Replica, Topics}, From, State) ->
  send_request(kafe_protocol_offset:request(Replica, Topics, State),
               From,
               fun kafe_protocol_offset:response/1,
               State);
handle_call({produce, Topic, Message, Options}, From, State) ->
  send_request(kafe_protocol_produce:request(Topic, Message, Options, State),
               From,
               fun kafe_protocol_produce:response/1,
               State);
handle_call({fetch, Replica, TopicName, Options}, From, State) ->
  send_request(kafe_protocol_fetch:request(Replica, TopicName, Options, State),
               From,
               fun kafe_protocol_fetch:response/1,
               State);
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(
  {tcp, _, <<Size:32/signed, Remainder/binary>> = Packet},
  #{parts := <<>>} = State
 ) when Size == byte_size(Remainder) ->
  process_response(Packet, State);
handle_info(
  {tcp, _, Part}, 
  #{parts := <<Size:32/signed, _/binary>> = Parts} = State
 ) when byte_size(<<Parts/binary, Part/binary>>) >= Size -> 
  <<Size:32/signed, Packet:Size/bytes, Remainder/binary>> = <<Parts/binary, Part/binary>>,
  process_response(<<Size:32, Packet/binary>>, maps:update(parts, Remainder, State));
handle_info(
  {tcp, _, Part}, 
  #{parts := Parts, socket := Socket} = State
 ) ->
  case inet:setopts(Socket, [{active, once}]) of
    ok ->
      {noreply, maps:update(parts, <<Parts/binary, Part/binary>>, State)};
    {error, _} = Reason ->
      {stop, Reason, State}
  end;
handle_info({tcp_closed, _}, State) ->
  {stop, abnormal, State};
handle_info(_Info, State) ->
  lager:info("--- handle_info ~p", [_Info]),
  lager:info("--- state ~p", [State]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

send_request(#{packet := Packet, state := State2}, 
             From, 
             Handler, 
             #{correlation_id := CorrelationId, 
               requests := Requests, 
               socket := Socket} = State1) ->
  case gen_tcp:send(Socket, Packet) of
    ok ->
      case inet:setopts(Socket, [{active, once}]) of
        ok ->
          {noreply, 
           maps:update(
             requests, 
             orddict:store(CorrelationId, 
                           #{from => From, handler => Handler}, 
                           Requests), 
             State2)};
        {error, _} = Error ->
          {stop, abnormal, Error, State1}
      end;
    {error, _} = Error ->
      {stop, abnormal, Error, State1}
  end.

process_response(
  <<Size:32/signed, Packet:Size/bytes>>,
  #{requests := Requests, socket := Socket} = State
 ) ->
  <<CorrelationId:32/signed, Remainder/bytes>> = Packet,
  case orddict:find(CorrelationId, Requests) of
    {ok, #{from := From, handler := ResponseHandler}} ->
      gen_server:reply(From, ResponseHandler(Remainder)),
      case inet:setopts(Socket, [{active, once}]) of
        ok ->
          {noreply, maps:update(requests, orddict:erase(CorrelationId, Requests), State)};
        {error, _} = Reason ->
          {stop, Reason, State}
      end;
    error ->
      case inet:setopts(Socket, [{active, once}]) of
        ok ->
          {noreply, State};
        {error, _} = Reason ->
          {stop, Reason, State}
      end
  end.
