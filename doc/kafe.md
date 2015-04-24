

# Module kafe #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

<a name="types"></a>

## Data Types ##




### <a name="type-attributes">attributes()</a> ###



<pre><code>
attributes() = integer()
</code></pre>





### <a name="type-broker">broker()</a> ###



<pre><code>
broker() = #{host =&gt; <a href="#type-host">host()</a>, id =&gt; <a href="#type-id">id()</a>, port =&gt; port()}
</code></pre>





### <a name="type-consumer_group">consumer_group()</a> ###



<pre><code>
consumer_group() = binary()
</code></pre>





### <a name="type-consumer_metadata">consumer_metadata()</a> ###



<pre><code>
consumer_metadata() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, coordinator_id =&gt; <a href="#type-coordinator_id">coordinator_id()</a>, coordinator_host =&gt; <a href="#type-host">host()</a>, coordinator_port =&gt; port()}
</code></pre>





### <a name="type-coordinator_id">coordinator_id()</a> ###



<pre><code>
coordinator_id() = integer()
</code></pre>





### <a name="type-crc">crc()</a> ###



<pre><code>
crc() = integer()
</code></pre>





### <a name="type-error_code">error_code()</a> ###



<pre><code>
error_code() = atom()
</code></pre>





### <a name="type-fetch_offset">fetch_offset()</a> ###



<pre><code>
fetch_offset() = integer()
</code></pre>





### <a name="type-fetch_options">fetch_options()</a> ###



<pre><code>
fetch_options() = #{partition =&gt; integer(), offset =&gt; integer(), max_bytes =&gt; integer(), min_bytes =&gt; integer(), max_wait_time =&gt; integer()}
</code></pre>





### <a name="type-high_watermaker_offset">high_watermaker_offset()</a> ###



<pre><code>
high_watermaker_offset() = integer()
</code></pre>





### <a name="type-host">host()</a> ###



<pre><code>
host() = binary()
</code></pre>





### <a name="type-id">id()</a> ###



<pre><code>
id() = integer()
</code></pre>





### <a name="type-isr">isr()</a> ###



<pre><code>
isr() = integer()
</code></pre>





### <a name="type-key">key()</a> ###



<pre><code>
key() = binary()
</code></pre>





### <a name="type-leader">leader()</a> ###



<pre><code>
leader() = integer()
</code></pre>





### <a name="type-max_bytes">max_bytes()</a> ###



<pre><code>
max_bytes() = integer()
</code></pre>





### <a name="type-message">message()</a> ###



<pre><code>
message() = <a href="#type-value">value()</a> | {<a href="#type-key">key()</a>, <a href="#type-value">value()</a>}
</code></pre>





### <a name="type-message_data">message_data()</a> ###



<pre><code>
message_data() = #{offset =&gt; <a href="#type-offset">offset()</a>, crc =&gt; <a href="#type-crc">crc()</a>, attributes =&gt; <a href="#type-attributes">attributes()</a>, key =&gt; <a href="#type-key">key()</a>, value =&gt; <a href="#type-value">value()</a>}
</code></pre>





### <a name="type-message_set">message_set()</a> ###



<pre><code>
message_set() = #{name =&gt; <a href="#type-topic_name">topic_name()</a>, partitions =&gt; [<a href="#type-partition_message">partition_message()</a>]}
</code></pre>





### <a name="type-metadata">metadata()</a> ###



<pre><code>
metadata() = #{brokers =&gt; [<a href="#type-broker">broker()</a>], topics =&gt; [<a href="#type-topic">topic()</a>]}
</code></pre>





### <a name="type-metadata_info">metadata_info()</a> ###



<pre><code>
metadata_info() = binary()
</code></pre>





### <a name="type-offset">offset()</a> ###



<pre><code>
offset() = integer()
</code></pre>





### <a name="type-offset_fetch_options">offset_fetch_options()</a> ###



<pre><code>
offset_fetch_options() = [<a href="#type-topic_name">topic_name()</a>] | [{<a href="#type-topic_name">topic_name()</a>, [<a href="#type-partition_number">partition_number()</a>]}]
</code></pre>





### <a name="type-offset_set">offset_set()</a> ###



<pre><code>
offset_set() = #{name =&gt; <a href="#type-topic_name">topic_name()</a>, partitions_offset =&gt; [<a href="#type-partition_offset_def">partition_offset_def()</a>]}
</code></pre>





### <a name="type-partition">partition()</a> ###



<pre><code>
partition() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, id =&gt; <a href="#type-id">id()</a>, isr =&gt; [<a href="#type-isr">isr()</a>], leader =&gt; <a href="#type-leader">leader()</a>, replicas =&gt; [<a href="#type-replicat">replicat()</a>]}
</code></pre>





### <a name="type-partition_def">partition_def()</a> ###



<pre><code>
partition_def() = {<a href="#type-partition_number">partition_number()</a>, <a href="#type-fetch_offset">fetch_offset()</a>, <a href="#type-max_bytes">max_bytes()</a>}
</code></pre>





### <a name="type-partition_info">partition_info()</a> ###



<pre><code>
partition_info() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, id =&gt; <a href="#type-id">id()</a>, offsets =&gt; [<a href="#type-offset">offset()</a>]}
</code></pre>





### <a name="type-partition_message">partition_message()</a> ###



<pre><code>
partition_message() = #{partition =&gt; <a href="#type-partition_number">partition_number()</a>, error_code =&gt; <a href="#type-error_code">error_code()</a>, high_watermaker_offset =&gt; <a href="#type-high_watermaker_offset">high_watermaker_offset()</a>, message =&gt; [<a href="#type-message_data">message_data()</a>]}
</code></pre>





### <a name="type-partition_number">partition_number()</a> ###



<pre><code>
partition_number() = integer()
</code></pre>





### <a name="type-partition_offset_def">partition_offset_def()</a> ###



<pre><code>
partition_offset_def() = #{partition =&gt; <a href="#type-partition_number">partition_number()</a>, offset =&gt; <a href="#type-offset">offset()</a>, metadata_info =&gt; <a href="#type-metadata_info">metadata_info()</a>, error_code =&gt; <a href="#type-error_code">error_code()</a>}
</code></pre>





### <a name="type-produce_options">produce_options()</a> ###



<pre><code>
produce_options() = #{timeout =&gt; integer(), required_acks =&gt; integer(), partition =&gt; integer()}
</code></pre>





### <a name="type-replicat">replicat()</a> ###



<pre><code>
replicat() = integer()
</code></pre>





### <a name="type-topic">topic()</a> ###



<pre><code>
topic() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, name =&gt; <a href="#type-topic_name">topic_name()</a>, partitions =&gt; [<a href="#type-partition">partition()</a>]}
</code></pre>





### <a name="type-topic_name">topic_name()</a> ###



<pre><code>
topic_name() = binary()
</code></pre>





### <a name="type-topic_partition_info">topic_partition_info()</a> ###



<pre><code>
topic_partition_info() = #{name =&gt; <a href="#type-topic_name">topic_name()</a>, partitions =&gt; [<a href="#type-partition_info">partition_info()</a>]}
</code></pre>





### <a name="type-topics">topics()</a> ###



<pre><code>
topics() = [<a href="#type-topic_name">topic_name()</a>] | [{<a href="#type-topic_name">topic_name()</a>, [<a href="#type-partition_def">partition_def()</a>]}]
</code></pre>





### <a name="type-value">value()</a> ###



<pre><code>
value() = binary()
</code></pre>


<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#consumer_metadata-1">consumer_metadata/1</a></td><td>
Consumer Metadata Request.</td></tr><tr><td valign="top"><a href="#fetch-1">fetch/1</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(-1, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-2">fetch/2</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(ReplicatID, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-3">fetch/3</a></td><td> 
Fetch messages.</td></tr><tr><td valign="top"><a href="#metadata-0">metadata/0</a></td><td>Equivalent to <a href="#metadata-1"><tt>metadata([])</tt></a>.</td></tr><tr><td valign="top"><a href="#metadata-1">metadata/1</a></td><td> 
Return metadata for the given topics.</td></tr><tr><td valign="top"><a href="#offset-1">offset/1</a></td><td>Equivalent to <a href="#offset-2"><tt>offset(-1, Topics)</tt></a>.</td></tr><tr><td valign="top"><a href="#offset-2">offset/2</a></td><td> 
Get offet for the given topics and replicat.</td></tr><tr><td valign="top"><a href="#offset_commit-6">offset_commit/6</a></td><td>
Offset commit.</td></tr><tr><td valign="top"><a href="#offset_fetch-2">offset_fetch/2</a></td><td>
Offset fetch.</td></tr><tr><td valign="top"><a href="#produce-2">produce/2</a></td><td>Equivalent to <a href="#produce-3"><tt>produce(Topic, Message, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#produce-3">produce/3</a></td><td> 
Send a message.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="consumer_metadata-1"></a>

### consumer_metadata/1 ###


<pre><code>
consumer_metadata(ConsumerGroup::<a href="#type-consumer_group">consumer_group()</a>) -&gt; {ok, <a href="#type-consumer_metadata">consumer_metadata()</a>}
</code></pre>
<br />


Consumer Metadata Request
<a name="fetch-1"></a>

### fetch/1 ###

`fetch(TopicName) -> any()`

Equivalent to [`fetch(-1, TopicName, #{})`](#fetch-3).
<a name="fetch-2"></a>

### fetch/2 ###

`fetch(ReplicatID, TopicName) -> any()`

Equivalent to [`fetch(ReplicatID, TopicName, #{})`](#fetch-3).
<a name="fetch-3"></a>

### fetch/3 ###


<pre><code>
fetch(ReplicatID::<a href="#type-replicat">replicat()</a>, TopicName::<a href="#type-topic_name">topic_name()</a>, Options::<a href="#type-fetch_options">fetch_options()</a>) -&gt; {ok, [<a href="#type-message_set">message_set()</a>]}
</code></pre>
<br />


 
Fetch messages


Options:

* `partition :: integer()` : The id of the partition the fetch is for (default : partition with the highiest offset).

* `offset :: integer()` : The offset to begin this fetch from (default : last offset for the partition)

* `max_bytes :: integer()` : The maximum bytes to include in the message set for this partition. This helps bound the size of the response (default :
1)/

* `min_bytes :: integer()` : This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0
the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is
set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in
combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting
MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding) (default :
1024*1024).

* `max_wait_time :: integer()` : The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available
at the time the request is issued (default : 1).




ReplicatID must __always__ be -1.


Example:

```

 Response = kafe:fetch(<<"topic">>)
 Response1 = kafe:fetch(<<"topic">>, #{offset => 2, partition => 3}).
```


For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI).
<a name="metadata-0"></a>

### metadata/0 ###

`metadata() -> any()`

Equivalent to [`metadata([])`](#metadata-1).
<a name="metadata-1"></a>

### metadata/1 ###


<pre><code>
metadata(Topics::[<a href="#type-topic_name">topic_name()</a>]) -&gt; {ok, <a href="#type-metadata">metadata()</a>}
</code></pre>
<br />


 
Return metadata for the given topics


Example:

```

 Metadata = kafe:metadata([<<"topic1">>, <<"topic2">>]).
```



This example return all metadata for `topic1` and `topic2`


For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest).
<a name="offset-1"></a>

### offset/1 ###

`offset(Topics) -> any()`

Equivalent to [`offset(-1, Topics)`](#offset-2).
<a name="offset-2"></a>

### offset/2 ###


<pre><code>
offset(ReplicatID::<a href="#type-replicat">replicat()</a>, Topics::<a href="#type-topics">topics()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
</code></pre>
<br />


 
Get offet for the given topics and replicat


Example:

```

 Offset = kafe:offet(-1, [<<"topic1">>, {<<"topic2">>, [{0, -1, 1}, {2, -1, 1}]}]).
```


For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest).
<a name="offset_commit-6"></a>

### offset_commit/6 ###

`offset_commit(Brocker, ConsumerGroup, ConsumerGroupGenerationId, ConsumerId, RetentionTime, X6) -> any()`


Offset commit
<a name="offset_fetch-2"></a>

### offset_fetch/2 ###


<pre><code>
offset_fetch(ConsumerGroup::<a href="#type-consumer_group">consumer_group()</a>, Options::<a href="#type-offset_fetch_options">offset_fetch_options()</a>) -&gt; {ok, [<a href="#type-offset_set">offset_set()</a>]}
</code></pre>
<br />


Offset fetch
<a name="produce-2"></a>

### produce/2 ###

`produce(Topic, Message) -> any()`

Equivalent to [`produce(Topic, Message, #{})`](#produce-3).
<a name="produce-3"></a>

### produce/3 ###


<pre><code>
produce(Topic::<a href="#type-topic_name">topic_name()</a>, Message::<a href="#type-message">message()</a>, Options::<a href="#type-produce_options">produce_options()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
</code></pre>
<br />


 
Send a message


Options:

* `timeout :: integer()` : This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in
RequiredAcks. The timeout is not an exact limit on the request time for a few reasons: (1) it does not include network latency, (2) the timer begins at the
beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, (3) we will not
terminate a local write so if the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client should use the
socket timeout. (default: 5000)

* `required_acks :: integer()` : This field indicates how many acknowledgements the servers should receive before responding to the request. If it is
0 the server will not send any response (this is the only case where the server will not reply to a request). If it is 1, the server will wait the data is
written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a
response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more
acknowledgements than there are in-sync replicas). (default: 0)

* `partition :: integer()` : The partition that data is being published to. (default: 0)



Example:

```

 Response = kafe:product(<<"topic">>, <<"a simple message">>, #{timeout => 1000, partition => 0}).
 Response1 = kafe:product(<<"topic">>, {<<"key">>, <<"Another simple message">>}).
```


For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI).
