%% @doc
%% @author Gregoire Lejeune <gl@finexkap.com>
%% @copyright 2014 Finexkap
%%
%% A Kafka client un pure Erlang
%% @end
-module(kafe).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-define(SERVER, ?MODULE).

-type error_code() :: atom().
-type id() :: integer().
-type replicat() :: integer().
-type leader() :: integer().
-type isr() :: integer().
-type partition() :: #{error_code => error_code(), id => id(), isr => [isr()], leader => leader(), replicas => [replicat()]}.
-type topic_name() :: binary().
-type topic() :: #{error_code => error_code(), name => topic_name(), partitions => [partition()]}.
-type host() :: binary().
-type broker() :: #{host => host(), id => id(), port => port()}.
-type metadata() :: #{brokers => [broker()], topics => [topic()]}.
-type max_bytes() :: integer().
-type fetch_offset() :: integer().
-type partition_number() :: integer().
-type partition_def() :: {partition_number(), fetch_offset(), max_bytes()}.
-type topics() :: [topic_name()] | [{topic_name(), [partition_def()]}].
-type offset() :: integer().
-type partition_info() :: #{error_code => error_code(), id => id(), offsets => [offset()]}.
-type topic_partition_info() :: #{name => topic_name(), partitions => [partition_info()]}.
-type key() :: binary().
-type value() :: binary().
-type message() :: value() | {key(), value()}.
-type produce_options() :: #{timeout => integer(), required_acks => integer(), partition => integer()}.
-type fetch_options() :: #{partition => integer(), offset => integer(), max_bytes => integer(), min_bytes => integer(), max_wait_time => integer()}.
-type high_watermaker_offset() :: integer().
-type attributes() :: integer().
-type crc() :: integer().
-type message_data() :: #{offset => offset(), crc => crc(), attributes => attributes(), key => key(), value => value()}.
-type partition_message() :: #{partition => partition_number(), error_code => error_code(), high_watermaker_offset => high_watermaker_offset(), message => [message_data()]}.
-type message_set() :: #{name => topic_name(), partitions => [partition_message()]}.

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

% @hidden
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

% @doc
% Return kafka metadata
% @end
-spec metadata() -> {ok, metadata()}.
metadata() ->
  gen_server:call(?SERVER, {metadata, []}).

% @doc
% Return metadata for the given topics
% @end
-spec metadata(topics()) -> {ok, metadata()}.
metadata(Topics) when is_list(Topics) ->
  gen_server:call(?SERVER, {metadata, Topics}).

% @doc
% Get offet for the given topics and replicat
% @end
-spec offset(replicat(), topics()) -> {ok, [topic_partition_info()]}.
offset(ReplicatID, Topics) ->
  gen_server:call(?SERVER, {offset, ReplicatID, Topics}).

% @equiv produce(Topic, Message, #{})
-spec produce(topic_name(), message()) -> {ok, [topic_partition_info()]}.
produce(Topic, Message) ->
  produce(Topic, Message, #{}).

% @doc
% Send a message
% @end
-spec produce(topic_name(), message(), produce_options()) -> {ok, [topic_partition_info()]}.
produce(Topic, Message, Options) ->
  gen_server:call(?SERVER, {produce, Topic, Message, Options}).

% @equiv fetch(ReplicatID, TopicName, #{})
-spec fetch(replicat(), topic_name()) -> {ok, [message_set()]}.
fetch(ReplicatID, TopicName) ->
  fetch(ReplicatID, TopicName, #{}).

% @doc
% Fetch messages
%
% > ReplicatID must *always* be -1
% @end
-spec fetch(replicat(), topic_name(), fetch_options()) -> {ok, [message_set()]}.
fetch(ReplicatID, TopicName, Options) ->
  gen_server:call(?SERVER, {fetch, ReplicatID, TopicName, Options}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

% @hidden
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

% @hidden
handle_call({metadata, Topics}, From, State) ->
  send_request(kafe_protocol_metadata:request(Topics, State), 
               From, 
               fun kafe_protocol_metadata:response/1, 
               State);
% @hidden
handle_call({offset, ReplicatID, Topics}, From, State) ->
  send_request(kafe_protocol_offset:request(ReplicatID, Topics, State),
               From,
               fun kafe_protocol_offset:response/1,
               State);
% @hidden
handle_call({produce, Topic, Message, Options}, From, State) ->
  send_request(kafe_protocol_produce:request(Topic, Message, Options, State),
               From,
               fun kafe_protocol_produce:response/1,
               State);
% @hidden
handle_call({fetch, ReplicatID, TopicName, Options}, From, State) ->
  send_request(kafe_protocol_fetch:request(ReplicatID, TopicName, Options, State),
               From,
               fun kafe_protocol_fetch:response/1,
               State);
% @hidden
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(
  {tcp, _, <<Size:32/signed, Remainder/binary>> = Packet},
  #{parts := <<>>} = State
 ) when Size == byte_size(Remainder) ->
  process_response(Packet, State);
% @hidden
handle_info(
  {tcp, _, Part}, 
  #{parts := <<Size:32/signed, _/binary>> = Parts} = State
 ) when byte_size(<<Parts/binary, Part/binary>>) >= Size -> 
  <<Size:32/signed, Packet:Size/bytes, Remainder/binary>> = <<Parts/binary, Part/binary>>,
  process_response(<<Size:32, Packet/binary>>, maps:update(parts, Remainder, State));
% @hidden
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
% @hidden
handle_info({tcp_closed, _}, State) ->
  {stop, abnormal, State};
% @hidden
handle_info(_Info, State) ->
  lager:info("--- handle_info ~p", [_Info]),
  lager:info("--- state ~p", [State]),
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% @hidden
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

% @hidden
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

