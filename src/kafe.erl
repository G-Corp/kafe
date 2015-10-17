% @author Grégoire Lejeune <gl@finexkap.com>
% @copyright 2014-2015 Finexkap
% @since 2014
% @doc
% A Kafka client for Erlang
% @end
-module(kafe).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-include_lib("kernel/include/inet.hrl").
-define(SERVER, ?MODULE).

% Public API
-export([
         metadata/0,
         metadata/1,
         offset/1,
         offset/2,
         produce/2,
         produce/3,
         fetch/1,
         fetch/2,
         fetch/3,
         consumer_metadata/1,
         offset_fetch/1,
         offset_fetch/2,
         offset_commit/2,
         offset_commit/4,
         offset_commit/5,
         offsets/2,
         offsets/3
        ]).

% Internal API
-export([
         start_link/0,
         first_broker/0,
         broker/2,
         broker_by_name/1,
         topics/0,
         max_offset/1,
         max_offset/2,
         partition_for_offset/2,
         api_version/0,
         state/0
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type error_code() :: no_error
                      | unknown
                      | offset_out_of_range
                      | invalid_message
                      | unknown_topic_or_partition
                      | invalid_message_size
                      | leader_not_available
                      | not_leader_for_partition
                      | request_timed_out
                      | broker_not_available
                      | replica_not_available
                      | message_size_too_large
                      | stale_controller_epoch
                      | offset_metadata_too_large
                      | offsets_load_in_progress
                      | consumer_coordinator_not_available
                      | not_coordinator_for_consumer.
-type metadata() :: #{brokers => [#{host => binary(), 
                                    id => integer(), 
                                    port => port()}], 
                      topics => [#{error_code => error_code(), 
                                   name => binary(), 
                                   partitions => [#{error_code => error_code(), 
                                                    id => integer(), 
                                                    isr => [integer()], 
                                                    leader => integer(), 
                                                    replicas => [integer()]}]}]}.
-type topics() :: [binary()] | [{binary(), [{integer(), integer(), integer()}]}].
-type topic_partition_info() :: #{name => binary(), partitions => [#{error_code => error_code(), id => integer(), offsets => [integer()]}]}.
-type message() :: binary() | {binary(), binary()}.
-type produce_options() :: #{timeout => integer(), required_acks => integer(), partition => integer()}.
-type fetch_options() :: #{partition => integer(), offset => integer(), max_bytes => integer(), min_bytes => integer(), max_wait_time => integer()}.
-type message_set() :: #{name => binary(), 
                         partitions => [#{partition => integer(), 
                                          error_code => error_code(), 
                                          high_watermaker_offset => integer(), 
                                          message => [#{offset => integer(), 
                                                        crc => integer(), 
                                                        attributes => integer(), 
                                                        key => binary(), 
                                                        value => binary()}]}]}.
-type consumer_metadata() :: #{error_code => error_code(), coordinator_id => integer(), coordinator_host => binary(),  coordinator_port => port()}.
-type offset_fetch_options() :: [binary()] | [{binary(), [integer()]}].
-type offset_fetch_set() :: #{name => binary(), 
                              partitions_offset => [#{partition => integer(), 
                                                      offset => integer(), 
                                                      metadata_info => binary(), 
                                                      error_code => error_code()}]}.
-type offset_commit_set() :: [#{name => binary(), 
                                partitions => [#{partition => integer(), 
                                                 error_code => error_code()}]}].
-type offset_commit_option() :: [{binary(), [{integer(), integer(), binary()}]}].
-type offset_commit_option_v1() :: [{binary(), [{integer(), integer(), integer(), binary()}]}].

% @hidden
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

% @hidden
first_broker() ->
  gen_server:call(?SERVER, first_broker, infinity).

% @hidden
broker(Topic, Partition) ->
  case gen_server:call(?SERVER, {broker, Topic, Partition}, infinity) of
    undefined -> first_broker();
    Broker -> Broker
  end.

% @hidden
broker_by_name(BrokerName) ->
  case gen_server:call(?SERVER, {broker_by_name, BrokerName}, infinity) of
    undefined -> first_broker();
    Broker -> Broker
  end.

% @hidden
topics() ->
  gen_server:call(?SERVER, topics, infinity).

% @hidden
max_offset(TopicName) ->
  case offset([TopicName]) of
    {ok, [#{partitions := Partitions}]} ->
      lists:foldl(fun(#{id := P, offsets := [O|_]}, {_, Offset} = Acc) ->
                      if
                        O > Offset -> {P, O};
                        true -> Acc
                      end
                  end, {?DEFAULT_OFFSET_PARTITION, 0}, Partitions);
    {ok, _} ->
      {?DEFAULT_OFFSET_PARTITION, 0}
  end.

% @hidden
max_offset(TopicName, Partition) ->
  case offset([{TopicName, [{Partition, ?DEFAULT_OFFSET_TIME, ?DEFAULT_OFFSET_MAX_SIZE}]}]) of
    {ok, 
     [#{partitions := [#{id := Partition, 
                         offsets := [Offset|_]}]}]
    } ->
      {Partition, Offset};
    {ok, _} ->
      {Partition, 0}
  end.

% @hidden
partition_for_offset(TopicName, Offset) ->
  case offset([TopicName]) of
    {ok, [#{partitions := Partitions}]} ->
      lists:foldl(fun(#{id := P, offsets := [O|_]}, {_, Offset1} = Acc) ->
                      if
                        O >= Offset1 -> {P, Offset1};
                        true -> Acc
                      end
                  end, {0, Offset}, Partitions);
    {ok, _} ->
      {?DEFAULT_OFFSET_PARTITION, Offset}
  end.

% @hidden
api_version() ->
  gen_server:call(?SERVER, api_version, infinity).

% @hidden
state() ->
  gen_server:call(?SERVER, state, infinity).


% @equiv metadata([])
metadata() ->
  metadata([]).

% @doc
% Return metadata for the given topics
%
% Example:
% <pre>
% Metadata = kafe:metadata([&lt;&lt;"topic1"&gt;&gt;, &lt;&lt;"topic2"&gt;&gt;]).
% </pre>
%
% This example return all metadata for <tt>topic1</tt> and <tt>topic2</tt>
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest">Kafka protocol documentation</a>.
% @end
-spec metadata([binary()]) -> {ok, metadata()}.
metadata(Topics) when is_list(Topics) ->
  kafe_protocol_metadata:run(Topics).

% @equiv offset(-1, Topics)
offset(Topics) when is_list(Topics)->
  offset(-1, Topics).

% @doc
% Get offet for the given topics and replicat
%
% Example:
% <pre>
% Offset = kafe:offet(-1, [&lt;&lt;"topic1"&gt;&gt;, {&lt;&lt;"topic2"&gt;&gt;, [{0, -1, 1}, {2, -1, 1}]}]).
% </pre>
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest">Kafka protocol documentation</a>.
% @end
-spec offset(integer(), topics()) -> {ok, [topic_partition_info()]}.
offset(ReplicatID, Topics) when is_integer(ReplicatID), is_list(Topics) ->
  kafe_protocol_offset:run(ReplicatID, Topics).

% @equiv produce(Topic, Message, #{})
produce(Topic, Message) ->
  produce(Topic, Message, #{}).

% @doc
% Send a message
%
% Options:
% <ul>
% <li><tt>timeout :: integer()</tt> : This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in
% RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include network latency, (2) the timer begins at the
% beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, (3) we will not
% terminate a local write so if the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client should use the
% socket timeout. (default: 5000)</li>
% <li><tt>required_acks :: integer()</tt> : This field indicates how many acknowledgements the servers should receive before responding to the request. If it is
% 0 the server will not send any response (this is the only case where the server will not reply to a request). If it is 1, the server will wait the data is
% written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a
% response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more
% acknowledgements than there are in-sync replicas). (default: 0)</li>
% <li><tt>partition :: integer()</tt> : The partition that data is being published to. (default: 0)</li>
% </ul>
%
% Example:
% <pre>
% Response = kafe:product(&lt;&lt;"topic"&gt;&gt;, &lt;&lt;"a simple message"&gt;&gt;, #{timeout =&gt; 1000, partition =&gt; 0}).
% Response1 = kafe:product(&lt;&lt;"topic"&gt;&gt;, {&lt;&lt;"key"&gt;&gt;, &lt;&lt;"Another simple message"&gt;&gt;}).
% </pre>
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI">Kafka protocol documentation</a>.
% @end
-spec produce(binary(), message(), produce_options()) -> {ok, [topic_partition_info()]}.
produce(Topic, Message, Options) ->
  kafe_protocol_produce:run(Topic, Message, Options).

% @equiv fetch(-1, TopicName, #{})
fetch(TopicName) when is_binary(TopicName) ->
  fetch(-1, TopicName, #{}).

% @equiv fetch(ReplicatID, TopicName, #{})
fetch(ReplicatID, TopicName) when is_integer(ReplicatID), is_binary(TopicName) ->
  fetch(ReplicatID, TopicName, #{});
% @equiv fetch(-1, TopicName, Options)
fetch(TopicName, Options) when is_binary(TopicName), is_map(Options) ->
  fetch(-1, TopicName, Options).


% @doc
% Fetch messages
%
% Options:
% <ul>
% <li><tt>partition :: integer()</tt> : The id of the partition the fetch is for (default : partition with the highiest offset).</li>
% <li><tt>offset :: integer()</tt> : The offset to begin this fetch from (default : last offset for the partition)</li>
% <li><tt>max_bytes :: integer()</tt> : The maximum bytes to include in the message set for this partition. This helps bound the size of the response (default :
% 1)/</li>
% <li><tt>min_bytes :: integer()</tt> : This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0
% the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is
% set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in
% combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting
% MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding) (default :
% 1024*1024).</li>
% <li><tt>max_wait_time :: integer()</tt> : The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available
% at the time the request is issued (default : 1).</li>
% </ul>
%
% ReplicatID must <b>always</b> be -1.
%
% Example:
% <pre>
% Response = kafe:fetch(&lt;&lt;"topic"&gt;&gt;)
% Response1 = kafe:fetch(&lt;&lt;"topic"&gt;&gt;, #{offset =&gt; 2, partition =&gt; 3}).
% </pre>
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI">Kafka protocol documentation</a>.
% @end
-spec fetch(integer(), binary(), fetch_options()) -> {ok, [message_set()]}.
fetch(ReplicatID, TopicName, Options) when is_integer(ReplicatID), is_binary(TopicName), is_map(Options) ->
  kafe_protocol_fetch:run(ReplicatID, TopicName, Options).

% @doc
% Consumer Metadata Request
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ConsumerMetadataRequest">Kafka protocol documentation</a>.
% @end
-spec consumer_metadata(binary()) -> {ok, consumer_metadata()}.
consumer_metadata(ConsumerGroup) ->
  kafe_protocol_consumer_metadata:run(ConsumerGroup).

% @doc
% Offset commit v0
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest">Kafka protocol documentation</a>.
% @end
-spec offset_commit(binary(), offset_commit_option()) -> {ok, [offset_commit_set()]}.
offset_commit(ConsumerGroup, Topics) ->
  kafe_protocol_consumer_offset_commit:run_v0(ConsumerGroup, 
                                              Topics).

% @doc
% Offset commit v1
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest">Kafka protocol documentation</a>.
% @end
-spec offset_commit(binary(), integer(), binary(), offset_commit_option_v1()) -> {ok, [offset_commit_set()]}.
offset_commit(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, Topics) ->
  kafe_protocol_consumer_offset_commit:run_v1(ConsumerGroup, 
                                              ConsumerGroupGenerationId, 
                                              ConsumerId, 
                                              Topics).

% @doc
% Offset commit v2
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest">Kafka protocol documentation</a>.
% @end
-spec offset_commit(binary(), integer(), binary(), integer(), offset_commit_option()) -> {ok, [offset_commit_set()]}.
offset_commit(ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, Topics) ->
  kafe_protocol_consumer_offset_commit:run_v2(ConsumerGroup, 
                                              ConsumerGroupGenerationId, 
                                              ConsumerId, 
                                              RetentionTime, 
                                              Topics).

% @equiv offset_fetch(ConsumerGroup, [])
-spec offset_fetch(binary()) -> {ok, [offset_fetch_set()]}.
offset_fetch(ConsumerGroup) ->
  offset_fetch(ConsumerGroup, []).

% @doc
% Offset fetch
%
% For more informations, see the <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest">Kafka protocol documentation</a>.
% @end
-spec offset_fetch(binary(), offset_fetch_options()) -> {ok, [offset_fetch_set()]}.
offset_fetch(ConsumerGroup, Options) ->
  kafe_protocol_consumer_offset_fetch:run(ConsumerGroup, Options).

% Private

% @hidden
init(_) ->
  KafkaBrokers = kafe_config:conf([kafe, brokers], 
                                     [
                                      {
                                       kafe_config:conf([kafe, host], ?DEFAULT_IP),
                                       kafe_config:conf([kafe, port], ?DEFAULT_PORT)
                                      }
                                     ]),
  ApiVersion = kafe_config:conf([kafe, api_version], ?DEFAULT_API_VERSION),
  CorrelationID = kafe_config:conf([kafe, correlation_id], ?DEFAULT_CORRELATION_ID),
  ClientID = kafe_config:conf([kafe, client_id], ?DEFAULT_CLIENT_ID),
  Offset = kafe_config:conf([kafe, offset], ?DEFAULT_OFFSET),
  BrokersUpdateFreq = kafe_config:conf([kafe, brokers_update_frequency], ?DEFAULT_BROKER_UPDATE),
  State = #{brokers => #{},
            brokers_list => [],
            topics => #{},
            brokers_update_frequency => BrokersUpdateFreq,
            api_version => ApiVersion,
            correlation_id => CorrelationID,
            client_id => ClientID,
            offset => Offset
           },
  State1 = update_state_with_metadata(get_connection(KafkaBrokers, State)),
  erlang:send_after(BrokersUpdateFreq, self(), update_brokers),
  {ok, State1}.

% @hidden
handle_call(first_broker, _From, State) ->
  {reply, get_first_broker(State), State};
handle_call({broker, Topic, Partition}, _From, #{topics := Topics, brokers := BrokersAddr} = State) ->
  case maps:get(Topic, Topics, undefined) of
    undefined ->
      {reply, undefined, State};
    Brokers ->
      case maps:get(Partition, Brokers, undefined) of
        undefined ->
          {reply, undefined, State};
        Broker ->
          {reply, maps:get(Broker, BrokersAddr, undefined), State}
      end
  end;
handle_call({broker_by_name, BrokerName}, _From, #{brokers := BrokersAddr} = State) -> 
  {reply, maps:get(eutils:to_string(BrokerName), BrokersAddr, undefined), State};
handle_call(topics, _From, #{topics := Topics} = State) ->
  {reply, Topics, State};
handle_call(api_version, _From, #{api_version := Version} = State) ->
  {reply, Version, State};
handle_call(state, _From, State) ->
  {reply, State, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(update_brokers, #{brokers_update_frequency := Frequency} = State) ->
  lager:debug("Update brokers list..."),
  State1 = update_state_with_metadata(remove_dead_brokers(State)),
  erlang:send_after(Frequency, self(), update_brokers),
  {noreply, State1};
% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% @hidden
get_connection([], State) -> 
  State;
get_connection([{Host, Port}|Rest], #{brokers_list := BrokersList, brokers := Brokers} = State) ->
  lager:debug("Get connection for ~s:~p", [Host, Port]),
  try
    case inet:gethostbyname(Host) of
      {ok, #hostent{h_name = Hostname, 
                    h_addrtype = AddrType,
                    h_addr_list = AddrsList}} -> 
        case get_host(AddrsList, Hostname, AddrType) of
          undefined ->
            lager:debug("Can't retrieve host for ~p:~p", [Host, Port]),
            get_connection(Rest, State);
          {BrokerAddr, BrokerHostList} ->
            case lists:foldl(fun(E, Acc) ->
                                 BrokerFullName = kafe_utils:broker_name(E, Port),
                                 case elists:include(BrokersList, BrokerFullName) of
                                   true -> Acc;
                                   _ -> [BrokerFullName|Acc]
                                 end
                             end, [], BrokerHostList) of
              [] ->
                lager:debug("All host already registered for ~p:~p", [enet:ip_to_str(BrokerAddr), Port]),
                get_connection(Rest, State);
              BrokerHostList1 ->
                case kafe_client_sup:start_child(BrokerAddr, Port) of
                  {ok, BrokerID} ->
                    lager:debug("Reference ~p to ~p", [BrokerID, BrokerHostList1]),
                    Brokers1 = lists:foldl(fun(BrokerHost, Acc) ->
                                               maps:put(BrokerHost, BrokerID, Acc)
                                           end, Brokers, BrokerHostList1),
                    get_connection(Rest, maps:put(brokers_list, BrokerHostList1 ++ BrokersList, maps:put(brokers, Brokers1, State)));
                  {error, Reason} ->
                    lager:debug("Connection faild to ~p:~p : ~p", [enet:ip_to_str(BrokerAddr), Port, Reason]),
                    get_connection(Rest, State)
                end
            end
        end;
      {error, Reason} ->
        lager:debug("Can't retrieve host by name for ~p:~p : ~p", [Host, Port, Reason]),
        get_connection(Rest, State)
    end
  catch
    Type:Reason1 ->
      lager:debug("Error while get connection for ~p:~p : ~p:~p", [Host, Port, Type, Reason1]),
      get_connection(Rest, State)
  end.

% @hidden
update_state_with_metadata(State) ->
  {ok, #{brokers := Brokers, 
         topics := Topics}} =  gen_server:call(get_first_broker(State),
                                               {call, 
                                                fun kafe_protocol_metadata:request/2, [[]],
                                                fun kafe_protocol_metadata:response/1},
                                               infinity),
  {Brokers1, State2} = lists:foldl(fun(#{host := Host, id := ID, port := Port}, {Acc, State1}) ->
                                       {maps:put(ID, kafe_utils:broker_name(Host, Port), Acc),
                                        get_connection([{eutils:to_string(Host), Port}], State1)}
                                   end, {#{}, State}, Brokers),
  State3 = remove_unlisted_brokers(maps:values(Brokers1), State2),
  Topics1 = lists:foldl(fun(#{name := Topic, partitions := Partitions}, Acc) ->
                            maps:put(Topic, 
                                     lists:foldl(fun(#{id := ID, leader := Leader}, Acc1) ->
                                                     maps:put(ID, maps:get(Leader, Brokers1), Acc1)
                                                 end, #{}, Partitions), 
                                     Acc)
                        end, #{}, Topics),
  maps:put(topics, Topics1, State3).

remove_unlisted_brokers(BrokersList, #{brokers := Brokers} = State) ->
  UnkillID = lists:foldl(fun(Broker, Acc) ->
                             case maps:get(Broker, Brokers, undefined) of
                               undefined -> Acc;
                               ID -> [ID|Acc]
                             end 
                         end, [], BrokersList),
  Brokers1 = maps:fold(fun(BrokerName, BrokerID, Acc) ->
                           case lists:member(BrokerName, BrokersList) of
                             true -> 
                               maps:put(BrokerName, BrokerID, Acc);
                             false ->
                               case lists:member(BrokerID, UnkillID) of
                                 true -> 
                                   ok;
                                 false ->
                                   _ = kafe_client_sup:stop_child(BrokerID)
                               end,
                               Acc
                           end
                       end, #{}, Brokers),
  State#{brokers => Brokers1, brokers_list => BrokersList}.


% @hidden
remove_dead_brokers(#{brokers_list := BrokersList} = State) ->
  lists:foldl(fun(Broker, #{brokers := Brokers1, brokers_list := BrokersList1} = State1) ->
                  case maps:get(Broker, Brokers1, undefined) of
                    undefined -> 
                      maps:put(brokers_list, 
                               lists:delete(Broker, BrokersList1), 
                               State1);
                    BrokerID ->
                      case gen_server:call(BrokerID, alive, infinity) of
                        ok -> 
                          State1;
                        {error, Reason} ->
                          _ = kafe_client_sup:stop_child(BrokerID),
                          lager:debug("Broker ~p not alive : ~p", [Broker, Reason]),
                          maps:put(brokers_list, 
                                   lists:delete(Broker, BrokersList1), 
                                   maps:put(brokers, maps:remove(Broker, Brokers1), State1))
                      end
                  end
              end, State, BrokersList).

% @hidden
get_host([], _, _) -> undefined;
get_host([Addr|Rest], Hostname, AddrType) ->
  case inet:getaddr(Hostname, AddrType) of
    {ok, Addr} -> 
      case inet:gethostbyaddr(Addr) of
        {ok, #hostent{h_name = Hostname1, h_aliases = HostAlias}} ->
          {Addr, lists:usort([Hostname|[Hostname1|HostAlias]])};
        _ ->
          {Addr, [Hostname]}
      end;
    _ -> get_host(Rest, Hostname, AddrType)
  end.

% @hidden
get_first_broker(#{brokers := Brokers}) ->
  get_first_broker(maps:values(Brokers));
get_first_broker([]) -> undefined;
get_first_broker([Broker|Rest]) ->
  case gen_server:call(Broker, alive, infinity) of
    ok -> Broker;
    {error, Reason} ->
      lager:debug("Broker ~p is not alive : ~p", [Broker, Reason]),
      get_first_broker(Rest)
  end.

% @doc
% Return the list of the next Nth unread offsets for a given topic and consumer group
% @end
-spec offsets(binary(), binary(), integer()) -> [{integer(), integer()}] | error.
offsets(TopicName, ConsumerGroup, Nth) ->
  NoError = kafe_error:code(0),
  case offset([TopicName]) of
    {ok, [#{name := TopicName, partitions := Partitions}]} ->
      {Offsets, PartitionsID} = lists:foldl(fun
                                              (#{id := PartitionID, 
                                                 offsets := [Offset|_], 
                                                 error_code := NoError1}, 
                                               {AccOffs, AccParts}) when NoError1 =:= NoError ->
                                                {[{PartitionID, Offset - 1}|AccOffs], [PartitionID|AccParts]};
                                              (_, Acc) ->
                                                Acc
                                            end, {[], []}, Partitions),
      case offset_fetch(ConsumerGroup, [{TopicName, PartitionsID}]) of
        {ok,[#{name := TopicName, partitions_offset := PartitionsOffset}]} ->
          CurrentOffsets = lists:foldl(fun
                                         (#{offset := Offset1, 
                                            partition := PartitionID1}, 
                                          Acc1) ->
                                           [{PartitionID1, Offset1 + 1}|Acc1];
                                          (_, Acc1) ->
                                            Acc1
                                       end, [], PartitionsOffset),
          CombinedOffsets = lists:foldl(fun({P, O}, Acc) ->
                                            case  lists:keyfind(P, 1, CurrentOffsets) of
                                              {P, C} when C < O -> [{P, O, C}|Acc];
                                              _ -> Acc
                                            end
                                        end, [], Offsets),
          lager:debug("Offsets = ~p / CurrentOffsets = ~p / CombinedOffsets = ~p", [Offsets, CurrentOffsets, CombinedOffsets]),
          {NewOffsets, Result} = get_offsets_list(CombinedOffsets, [], [], Nth),
          lists:foldl(fun({PartitionID, NewOffset}, Acc) ->
                          case offset_commit(ConsumerGroup, 
                                             [{TopicName, [{PartitionID, NewOffset, <<>>}]}]) of
                            {ok, [#{name := TopicName, 
                                    partitions := [#{partition := PartitionID, 
                                                     error_code := ErrorCode}]}]} when ErrorCode =:= NoError ->
                              Acc;
                            _ ->
                              delete_offset_for_partition(PartitionID, Acc) 
                          end
                      end, Result, NewOffsets);
        _ ->
          lager:info("Can't retriece offsets for consumer group ~p on topic ~p", [ConsumerGroup, TopicName]),
          error
      end;
    _ ->  
      lager:info("Can't retriece offsets for topic ~p", [TopicName]),
      error
  end.

% @doc
% Return the list of all unread offsets for a given topic and consumer group
% @end
-spec offsets(binary(), binary()) -> [{integer(), integer()}] | error.
offsets(TopicName, ConsumerGroup) ->
  offsets(TopicName, ConsumerGroup, -1).

get_offsets_list(Offsets, Result, Final, Nth) when Offsets =/= [], length(Result) =/= Nth ->
  [{PartitionID, MaxOffset, CurrentOffset}|SortedOffsets] = lists:sort(fun({_, O1, C1}, {_, O2, C2}) -> (C1 < C2) and (O1 < O2) end, Offsets),
  Offsets1 = if
               CurrentOffset + 1 > MaxOffset -> SortedOffsets;
               true  -> [{PartitionID, MaxOffset, CurrentOffset + 1}|SortedOffsets]
             end,
  Final1 = lists:keystore(PartitionID, 1, Final, {PartitionID, CurrentOffset}),
  get_offsets_list(Offsets1, [{PartitionID, CurrentOffset}|Result], Final1, Nth);
get_offsets_list(_, Result, Final, _) -> {Final, lists:reverse(Result)}.

delete_offset_for_partition(PartitionID, Offsets) ->
  case lists:keyfind(PartitionID, 1, Offsets) of
    false -> Offsets;
    _ -> delete_offset_for_partition(PartitionID, lists:keydelete(PartitionID, 1, Offsets))
  end.

