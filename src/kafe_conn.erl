% @hidden
-module(kafe_conn).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
%-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Addr, Port) ->
  gen_server:start_link(?MODULE, {Addr, Port}, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Addr, Port}) ->
  SndBuf = doteki:get_env([kafe, socket, sndbuf], ?DEFAULT_SOCKET_SNDBUF),
  RecBuf = doteki:get_env([kafe, socker, recbuf], ?DEFAULT_SOCKET_RECBUF),
  Buffer = lists:max([SndBuf, RecBuf, doteki:get_env([kafe, socket, buffer], max(SndBuf, RecBuf))]),
  case gen_tcp:connect(Addr, Port, [{mode, binary},
                                    {active, once},
                                    {sndbuf, SndBuf},
                                    {recbuf, RecBuf},
                                    {buffer, Buffer}]) of
    {ok, Socket} ->
      lager:debug("Connect to broker @ ~s:~p", [bucinet:ip_to_string(Addr), Port]),
      ApiVersion = doteki:get_env([kafe, api_version], ?DEFAULT_API_VERSION),
      CorrelationID = doteki:get_env([kafe, correlation_id], ?DEFAULT_CORRELATION_ID),
      ClientID = doteki:get_env([kafe, client_id], ?DEFAULT_CLIENT_ID),
      {ok, #{
         ip => Addr,
         port => Port,
         socket => Socket,
         api_version => ApiVersion,
         correlation_id => CorrelationID,
         client_id => ClientID,
         requests => orddict:new(),
         parts => <<>>,
         sndbuf => SndBuf,
         recbuf => RecBuf,
         buffer => Buffer
        }};
    {error, Reason} ->
      lager:debug("Connection faild to ~p:~p : ~p", [bucinet:ip_to_string(Addr), Port, Reason]),
      {stop, Reason}
  end.

handle_call({call, Request, RequestParams, Response}, From, State) ->
  send_request(erlang:apply(Request, RequestParams ++ [State]),
               From,
               Response,
               State);
handle_call(alive, _From, #{socket := Socket, sndbuf := SndBuf, recbuf := RecBuf, buffer := Buffer} = State) ->
  case inet:setopts(Socket, [{active, once}, {sndbuf, SndBuf}, {recbuf, RecBuf}, {buffer, Buffer}]) of
    ok ->
      {reply, ok, State};
    {error, _} = Reason ->
      {reply, Reason, State}
  end;
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(
  {tcp, _, <<Size:32/signed, Remainder/binary>> = Packet},
  #{parts := <<>>} = State
 ) when Size =< byte_size(Remainder) ->
  <<Size:32/signed, Packet1:Size/bytes, _Remainder1/binary>> = Packet,
  kafe_protocol:response(<<Size:32, Packet1/binary>>, maps:update(parts, <<>>, State));
handle_info(
  {tcp, _, Part},
  #{parts := <<Size:32/signed, CParts/binary>> = Parts} = State
 ) when byte_size(<<CParts/binary, Part/binary>>) >= Size ->
  <<Size:32/signed, Packet:Size/bytes, _Remainder/binary>> = <<Parts/binary, Part/binary>>,
  kafe_protocol:response(<<Size:32, Packet/binary>>, maps:update(parts, <<>>, State));
handle_info(
  {tcp, Socket, Part},
  #{parts := Parts, sndbuf := SndBuf, recbuf := RecBuf, buffer := Buffer} = State
 ) ->
  case inet:setopts(Socket, [{active, once}, {sndbuf, SndBuf}, {recbuf, RecBuf}, {buffer, Buffer}]) of
    ok ->
      {noreply, maps:update(parts, <<Parts/binary, Part/binary>>, State)};
    {error, _} = Reason ->
      {stop, Reason, State}
  end;
handle_info({tcp_closed, Socket}, State) ->
  lager:debug("Connections close ~p ...", [Socket]),
  {stop, normal, State};
handle_info(Info, State) ->
  lager:debug("Invalid message : ~p", [Info]),
  lager:debug("--- State ~p", [State]),
  {noreply, State}.

terminate(_Reason, #{socket := Socket}) ->
  lager:debug("Terminate..."),
  _ = gen_tcp:close(Socket),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

send_request(#{packet := Packet, state := State2, api_version := ApiVersion},
             From,
             Handler,
             #{correlation_id := CorrelationId,
               requests := Requests,
               socket := Socket,
               sndbuf := SndBuf,
               recbuf := RecBuf,
               buffer := Buffer} = State1) ->
  case gen_tcp:send(Socket, Packet) of
    ok ->
      case inet:setopts(Socket, [{active, once}, {sndbuf, SndBuf}, {recbuf, RecBuf}, {buffer, Buffer}]) of
        ok ->
          {noreply,
           maps:update(
             requests,
             orddict:store(CorrelationId,
                           #{from => From, handler => Handler, socket => Socket, api_version => ApiVersion},
                           Requests),
             State2)};
        {error, _} = Error ->
          {stop, abnormal, Error, State1}
      end;
    {error, _} = Error ->
      {stop, abnormal, Error, State1}
  end.

