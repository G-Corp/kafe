% @hidden
-module(kafe_client).
-behaviour(gen_server).

-include("../include/kafe.hrl").
%-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(ClientID, Addr, Port) ->
  gen_server:start_link({local, ClientID}, ?MODULE, {Addr, Port}, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Addr, Port}) ->
  case gen_tcp:connect(Addr, Port, [{mode, binary}, {active, once}]) of
    {ok, Socket} ->
      lager:info("Connect to broker @ ~s:~p", [enet:ip_to_str(Addr), Port]),
      ApiVersion = application:get_env(kafe, api_version, ?DEFAULT_API_VERSION),
      CorrelationID = application:get_env(kafe, correlation_id, ?DEFAULT_CORRELATION_ID),
      ClientID = application:get_env(kafe, client_id, ?DEFAULT_CLIENT_ID),
      {ok, #{
         ip => Addr,
         port => Port,
         socket => Socket,
         api_version => ApiVersion,
         correlation_id => CorrelationID,
         client_id => ClientID,
         requests => orddict:new(),
         parts => <<>>
        }};
    {error, Reason} ->
      lager:info("Connection faild to ~p:~p : ~p", [enet:ip_to_str(Addr), Port, Reason]),
      {stop, Reason}
  end.

handle_call({call, Request, RequestParams, Response}, From, State) ->
  send_request(erlang:apply(Request, RequestParams ++ [State]),
               From,
               Response,
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
  {tcp, Socket, Part}, 
  #{parts := Parts} = State
 ) ->
  case inet:setopts(Socket, [{active, once}]) of
    ok ->
      {noreply, maps:update(parts, <<Parts/binary, Part/binary>>, State)};
    {error, _} = Reason ->
      {stop, Reason, State}
  end;
handle_info({tcp_closed, Socket}, State) ->
  lager:info("Connections close ~p ...", [Socket]),
  {noreply, State};
handle_info(Info, State) ->
  lager:info("Invalid message : ~p", [Info]),
  lager:info("--- State ~p", [State]),
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
                           #{from => From, handler => Handler, socket => Socket}, 
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
  #{requests := Requests} = State
 ) ->
  <<CorrelationId:32/signed, Remainder/bytes>> = Packet,
  case orddict:find(CorrelationId, Requests) of
    {ok, #{from := From, handler := ResponseHandler, socket := Socket}} ->
      _ = gen_server:reply(From, ResponseHandler(Remainder)),
      case inet:setopts(Socket, [{active, once}]) of
        ok ->
          {noreply, maps:update(requests, orddict:erase(CorrelationId, Requests), State)};
        {error, _} = Reason ->
          {stop, Reason, State}
      end;
    error ->
      {noreply, State} %;
  end.

