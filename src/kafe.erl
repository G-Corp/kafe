%% @doc
%% @author Gregoire Lejeune (gl@finexkap.com)
%% @copyright 2014-2015 Finexkap
%%
%% A Kafka client in pure Erlang
%% @end
-module(kafe).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-include_lib("kernel/include/inet.hrl").
-define(SERVER, ?MODULE).

-type error_code() :: atom().
-type id() :: integer().
-type replicat() :: integer().
-type leader() :: integer().
-type isr() :: integer().
-type topic_name() :: binary().
-type host() :: binary().
-type max_bytes() :: integer().
-type fetch_offset() :: integer().
-type partition_number() :: integer().
-type offset() :: integer().
-type key() :: binary().
-type value() :: binary().
-type high_watermaker_offset() :: integer().
-type attributes() :: integer().
-type crc() :: integer().
-type consumer_group() :: binary().
-type coordinator_id() :: integer().
-type metadata_info() :: binary().

-type partition() :: #{error_code => error_code(), id => id(), isr => [isr()], leader => leader(), replicas => [replicat()]}.
-type topic() :: #{error_code => error_code(), name => topic_name(), partitions => [partition()]}.
-type broker() :: #{host => host(), id => id(), port => port()}.
-type metadata() :: #{brokers => [broker()], topics => [topic()]}.
-type partition_def() :: {partition_number(), fetch_offset(), max_bytes()}.
-type topics() :: [topic_name()] | [{topic_name(), [partition_def()]}].
-type partition_info() :: #{error_code => error_code(), id => id(), offsets => [offset()]}.
-type topic_partition_info() :: #{name => topic_name(), partitions => [partition_info()]}.
-type message() :: value() | {key(), value()}.
-type produce_options() :: #{timeout => integer(), required_acks => integer(), partition => integer()}.
-type fetch_options() :: #{partition => integer(), offset => integer(), max_bytes => integer(), min_bytes => integer(), max_wait_time => integer()}.
-type message_data() :: #{offset => offset(), crc => crc(), attributes => attributes(), key => key(), value => value()}.
-type partition_message() :: #{partition => partition_number(), error_code => error_code(), high_watermaker_offset => high_watermaker_offset(), message => [message_data()]}.
-type message_set() :: #{name => topic_name(), partitions => [partition_message()]}.
-type consumer_metadata() :: #{error_code => error_code(), coordinator_id => coordinator_id(), coordinator_host => host(),  coordinator_port => port()}.
-type coordinator() :: {host(), port()}.
-type offset_fetch_options() :: [topic_name()] | [{topic_name(), [partition_number()]}].
-type partition_offset_def() :: #{partition => partition_number(), offset => offset(), metadata_info => metadata_info(), error_code => error_code()}.
-type offset_set() :: #{name => topic_name(), partitions_offset => [partition_offset_def()]}.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([
         add_broker/2,
         brockers/0,
         metadata/0,
         metadata/1,
         offset/2,
         produce/2,
         produce/3,
         fetch/2,
         fetch/3,
         consumer_metadata/1,
         offset_fetch/3
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
% @end
-spec add_broker(host(), port()) -> ok.
add_broker(Host, Port) ->
  gen_server:call(?SERVER, {add_broker, Host, Port}, infinity).

% @doc
% @end
-spec brockers() -> {ok, [string()]}.
brockers() ->
  gen_server:call(?SERVER, brokers, infinity).

% @doc
% Return kafka metadata
%
% [protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest)
% @end
-spec metadata() -> {ok, metadata()}.
metadata() ->
  gen_server:call(?SERVER, {metadata, []}, infinity).

% @doc
% Return metadata for the given topics
% @end
-spec metadata(topics()) -> {ok, metadata()}.
metadata(Topics) when is_list(Topics) ->
  gen_server:call(?SERVER, {metadata, Topics}, infinity).

% @doc
% Get offet for the given topics and replicat
% @end
-spec offset(replicat(), topics()) -> {ok, [topic_partition_info()]}.
offset(ReplicatID, Topics) ->
  gen_server:call(?SERVER, {offset, ReplicatID, Topics}, infinity).

% @equiv produce(Topic, Message, #{})
-spec produce(topic_name(), message()) -> {ok, [topic_partition_info()]}.
produce(Topic, Message) ->
  produce(Topic, Message, #{}).

% @doc
% Send a message
% @end
-spec produce(topic_name(), message(), produce_options()) -> {ok, [topic_partition_info()]}.
produce(Topic, Message, Options) ->
  gen_server:call(?SERVER, {produce, Topic, Message, Options}, infinity).

% @equiv fetch(ReplicatID, TopicName, #{})
-spec fetch(replicat(), topic_name()) -> {ok, [message_set()]}.
fetch(ReplicatID, TopicName) ->
  fetch(ReplicatID, TopicName, #{}).

% @doc
% Fetch messages
%
% ReplicatID must *always* be -1
% @end
-spec fetch(replicat(), topic_name(), fetch_options()) -> {ok, [message_set()]}.
fetch(ReplicatID, TopicName, Options) ->
  gen_server:call(?SERVER, {fetch, ReplicatID, TopicName, Options}, infinity).

% @doc
% Consumer Metadata Request
% @end
-spec consumer_metadata(consumer_group()) -> {ok, consumer_metadata()}.
consumer_metadata(ConsumerGroup) ->
  gen_server:call(?SERVER, {consumer_metadata, ConsumerGroup}, infinity).

% @doc
% Offset commit
% @end
offset_commit(Brocker, ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, [TopicName, [Partition, Offset, Metadata]]) ->
  todo.

% @doc
% Offset fetch
% @end
-spec offset_fetch(coordinator(), consumer_group(), offset_fetch_options()) -> {ok, [offset_set()]}.
offset_fetch(Coordinator, ConsumerGroup, Options) ->
  gen_server:call(?SERVER, {offset_fetch, Coordinator, ConsumerGroup, Options}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

% @hidden
init(_) ->
  KafkaBrokers = application:get_env(kafe, brokers, 
                                     [
                                      {
                                       application:get_env(kafe, host, ?DEFAULT_IP),
                                       application:get_env(kafe, port, ?DEFAULT_PORT)
                                      }
                                     ]),
  ClientID = application:get_env(kafe, client_id, ?DEFAULT_CLIENT_ID),
  CorrelationID = application:get_env(kafe, correlation_id, ?DEFAULT_CORRELATION_ID),
  ApiVersion = application:get_env(kafe, api_version, ?DEFAULT_API_VERSION),
  Offset = application:get_env(kafe, offset, ?DEFAULT_OFFSET),
  BrokersUpdateFreq = application:get_env(kafe, brokers_update_frequency, ?DEFAULT_BROKER_UPDATE),
  State = #{brockers_list => [],
            brokers => #{},
            brokers_update_frequency => BrokersUpdateFreq,
            kafka_brokers => KafkaBrokers,
            api_version => ApiVersion,
            correlation_id => CorrelationID,
            client_id => ClientID,
            requests => orddict:new(),
            parts => <<>>,
            offset => Offset
           },
  State1 = get_connection(State),
  lager:debug("Init state = ~p", [State1]),
  {ok, State1}.

% @hidden
handle_call(brokers, _From, #{brockers_list := Brokers} = State) ->
  {reply, {ok, Brokers}, State};
% @hidden
handle_call({add_broker, Host, Port}, _From, State) ->
  {reply, ok, get_connection([{Host, Port}], State)};
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
handle_call({consumer_metadata, ConsumerGroup}, From, State) ->
  send_request(kafe_protocol_consumer_metadata:request(ConsumerGroup, State),
               From,
               fun kafe_protocol_consumer_metadata:response/1,
               State);
% @hidden
handle_call({offset_fetch, Coordinator, ConsumerGroup, Options}, From, State) ->
  send_request(kafe_protocol_consumer_offset_fetch:request(ConsumerGroup, Options, State),
               From,
               fun kafe_protocol_consumer_offset_fetch:response/1,
               State,
               Coordinator);
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
  {tcp, Socket, Part}, 
  #{parts := Parts} = State
 ) ->
  case inet:setopts(Socket, [{active, once}]) of
    ok ->
      {noreply, maps:update(parts, <<Parts/binary, Part/binary>>, State)};
    {error, _} = Reason ->
      {stop, Reason, State}
  end;
% @hidden
handle_info({tcp_closed, Socket}, State) ->
  lager:info("Connections close ~p ...", [Socket]),
  {noreply, get_connection(State)};
% @hidden
handle_info(update_brokers, #{brokers_update_frequency := Frequency} = State) ->
  lager:info("Update brokers list..."),
  erlang:send_after(Frequency, self(), update_brokers),
  {noreply, State};
% @hidden
handle_info(Info, State) ->
  lager:info("Invalid message : ~p", [Info]),
  lager:info("--- State ~p", [State]),
  {noreply, State}.

% @hidden
terminate(_Reason, #{brockers_list := BrokersList, brokers := BrokersInfo}) ->
  lager:debug("Terminate !!!"),
  _ = lists:foreach(fun(Broker) ->
                        case maps:get(Broker, BrokersInfo, undefined) of
                          #{socket := Socket} ->
                            gen_tcp:close(Socket);
                          _ -> pass
                        end
                    end, BrokersList),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% @hidden
send_request(Request, From, Handler, State) ->
  send_request(Request, From, Handler, State, undefined).

% @hidden
send_request(#{packet := Packet, state := State2}, 
             From, 
             Handler, 
             #{correlation_id := CorrelationId, 
               requests := Requests, 
               brockers_list := BrokersList,
               brokers := BrokersInfo} = State1,
            Coordinator) ->
  lager:debug("Coordinator = ~p", [Coordinator]),
  case get_socket(BrokersList, BrokersInfo, broker_name(Coordinator)) of
    error ->
      {stop, abnormal, no_available_socket, State1};
    Socket ->
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
      end
  end.

% @hidden
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

% @hidden
get_connection(#{kafka_brokers := KafkaBrokers} = State) ->
  get_connection(KafkaBrokers, State).

% @hidden
get_connection([], State) -> 
  State;
get_connection([{Host, Port}|KafkaBrokers], #{brockers_list := BrokersList,
                                              brokers := BrokersInfo} = State) ->
  lager:debug("Get connection for ~p:~p", [Host, Port]),
  try
    case inet:gethostbyname(Host) of
      {ok, #hostent{h_name = Hostname, 
                    h_addrtype = AddrType,
                    h_addr_list = AddrsList}} -> 
        case get_host(AddrsList, Hostname, AddrType) of
          undefined ->
            lager:debug("Can't retrieve host for ~p:~p", [Host, Port]),
            get_connection(KafkaBrokers, State);
          {BrokerAddr, BrokerHostList} ->
            case lists:foldl(fun(E, Acc) ->
                                 BrockerFullName = broker_name(E, Port),
                                 case elists:include(BrokersList, BrockerFullName) of
                                   true -> Acc;
                                   _ -> [BrockerFullName|Acc]
                                 end
                             end, [], BrokerHostList) of
              [] ->
                lager:debug("All host already registered for ~p:~p", [enet:ip_to_str(BrokerAddr), Port]),
                get_connection(KafkaBrokers, State);
              BrokerHostList1 ->
                case gen_tcp:connect(BrokerAddr, Port, [{mode, binary}, {active, once}]) of
                  {ok, Socket} ->
                    lager:info("Start connection with broker @ ~s:~p / ~p", 
                               [enet:ip_to_str(BrokerAddr), Port, BrokerHostList1]),
                    State1 = maps:put(brockers_list, BrokerHostList1 ++ BrokersList, State),
                    State2 = lists:foldl(fun(BrokerHost, StateAcc) ->
                                             maps:put(
                                               brokers, 
                                               maps:put(BrokerHost, #{ 
                                                          host => BrokerHost,
                                                          addr => BrokerAddr,
                                                          port => Port,
                                                          socket => Socket
                                                         }, BrokersInfo),
                                               StateAcc
                                              )
                                         end, State1, BrokerHostList1),
                    get_connection(KafkaBrokers, State2);
                  {error, Reason} ->
                    lager:debug("Connection faild to ~p:~p : ~p", [enet:ip_to_str(BrokerAddr), Port, Reason]),
                    get_connection(KafkaBrokers, State)
                end
            end
        end;
      {error, Reason} ->
        lager:debug("Can't retrieve host by name for ~p:~p : ~p", [Host, Port, Reason]),
        get_connection(KafkaBrokers, State)
    end
  catch
    Type:Reason1 ->
      lager:debug("Error while get connection for ~p:~p : ~p:~p", [Host, Port, Type, Reason1]),
      get_connection(KafkaBrokers, State)
  end.

% @hidden
broker_name(undefined) -> undefined;
broker_name({Host, Port}) -> broker_name(Host, Port).
% @hidden
broker_name(Host, Port) ->
  eutils:to_string(Host) ++ ":" ++ eutils:to_string(Port). 

% @hidden
get_host([], _, _) -> undefined;
get_host([Addr|Rest], Hostname, AddrType) ->
  case inet:getaddr(Hostname, AddrType) of
    {ok, Addr} -> 
      case inet:gethostbyaddr(Addr) of
        {ok, #hostent{h_name = Hostname1, h_aliases = HostAlias}} ->
          {Addr, lists:usort([Hostname|[Hostname1|HostAlias]])};
        _ ->
          {Addr, Hostname}
      end;
    _ -> get_host(Rest, Hostname, AddrType)
  end.

% @hidden
get_socket([], _, _) -> error;
get_socket([Broker|Rest], BrokersInfo, undefined) ->
  case get_socket(<<>>, BrokersInfo, Broker) of
    error ->
      get_socket(Rest, BrokersInfo, undefined);
    Socket ->
      Socket
  end;
get_socket(_, BrokersInfo, Coordinator) ->
  lager:debug("Retrieve socket for ~p", [Coordinator]),
  case maps:get(Coordinator, BrokersInfo, undefined) of
    undefined -> 
      lager:debug("Socket undefined for coordinator ~p", [Coordinator]),
      error;
    #{socket := Socket} ->
      case inet:setopts(Socket, [{active, once}]) of
        ok ->
          lager:debug("Socket found for ~p", [Coordinator]),
          Socket;
        {error, _} ->
          lager:debug("Socket faild for ~p", [Coordinator]),
          error
      end
  end.
