

# Module kafe #
* [Description](#description)
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

A Kafka client for Erlang.

Copyright (c) 2014-2015 Finexkap

__Introduced in:__ 2014

__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Grégoire Lejeune ([`gl@finexkap.com`](mailto:gl@finexkap.com)).

<a name="types"></a>

## Data Types ##




### <a name="type-consumer_metadata">consumer_metadata()</a> ###


<pre><code>
consumer_metadata() = #{error_code =&gt; <a href="#type-error_code">error_code()</a>, coordinator_id =&gt; integer(), coordinator_host =&gt; binary(), coordinator_port =&gt; port()}
</code></pre>




### <a name="type-error_code">error_code()</a> ###


<pre><code>
error_code() = no_error | unknown | offset_out_of_range | invalid_message | unknown_topic_or_partition | invalid_message_size | leader_not_available | not_leader_for_partition | request_timed_out | broker_not_available | replica_not_available | message_size_too_large | stale_controller_epoch | offset_metadata_too_large | offsets_load_in_progress | consumer_coordinator_not_available | not_coordinator_for_consumer
</code></pre>




### <a name="type-fetch_options">fetch_options()</a> ###


<pre><code>
fetch_options() = #{partition =&gt; integer(), offset =&gt; integer(), max_bytes =&gt; integer(), min_bytes =&gt; integer(), max_wait_time =&gt; integer()}
</code></pre>




### <a name="type-message">message()</a> ###


<pre><code>
message() = binary() | {binary(), binary()}
</code></pre>




### <a name="type-message_set">message_set()</a> ###


<pre><code>
message_set() = #{name =&gt; binary(), partitions =&gt; [#{partition =&gt; integer(), error_code =&gt; <a href="#type-error_code">error_code()</a>, high_watermaker_offset =&gt; integer(), message =&gt; [#{offset =&gt; integer(), crc =&gt; integer(), attributes =&gt; integer(), key =&gt; binary(), value =&gt; binary()}]}]}
</code></pre>




### <a name="type-metadata">metadata()</a> ###


<pre><code>
metadata() = #{brokers =&gt; [#{host =&gt; binary(), id =&gt; integer(), port =&gt; port()}], topics =&gt; [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, name =&gt; binary(), partitions =&gt; [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, id =&gt; integer(), isr =&gt; [integer()], leader =&gt; integer(), replicas =&gt; [integer()]}]}]}
</code></pre>




### <a name="type-offset_commit_option">offset_commit_option()</a> ###


<pre><code>
offset_commit_option() = [{binary(), [{integer(), integer(), binary()}]}]
</code></pre>




### <a name="type-offset_commit_option_v1">offset_commit_option_v1()</a> ###


<pre><code>
offset_commit_option_v1() = [{binary(), [{integer(), integer(), integer(), binary()}]}]
</code></pre>




### <a name="type-offset_commit_set">offset_commit_set()</a> ###


<pre><code>
offset_commit_set() = [#{name =&gt; binary(), partitions =&gt; [#{partition =&gt; integer(), error_code =&gt; <a href="#type-error_code">error_code()</a>}]}]
</code></pre>




### <a name="type-offset_fetch_options">offset_fetch_options()</a> ###


<pre><code>
offset_fetch_options() = [binary()] | [{binary(), [integer()]}]
</code></pre>




### <a name="type-offset_fetch_set">offset_fetch_set()</a> ###


<pre><code>
offset_fetch_set() = #{name =&gt; binary(), partitions_offset =&gt; [#{partition =&gt; integer(), offset =&gt; integer(), metadata_info =&gt; binary(), error_code =&gt; <a href="#type-error_code">error_code()</a>}]}
</code></pre>




### <a name="type-produce_options">produce_options()</a> ###


<pre><code>
produce_options() = #{timeout =&gt; integer(), required_acks =&gt; integer(), partition =&gt; integer()}
</code></pre>




### <a name="type-topic_partition_info">topic_partition_info()</a> ###


<pre><code>
topic_partition_info() = #{name =&gt; binary(), partitions =&gt; [#{error_code =&gt; <a href="#type-error_code">error_code()</a>, id =&gt; integer(), offsets =&gt; [integer()]}]}
</code></pre>




### <a name="type-topics">topics()</a> ###


<pre><code>
topics() = [binary()] | [{binary(), [{integer(), integer(), integer()}]}]
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#consumer_metadata-1">consumer_metadata/1</a></td><td> 
Consumer Metadata Request.</td></tr><tr><td valign="top"><a href="#fetch-1">fetch/1</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(-1, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-2">fetch/2</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(ReplicatID, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-3">fetch/3</a></td><td> 
Fetch messages.</td></tr><tr><td valign="top"><a href="#metadata-0">metadata/0</a></td><td>Equivalent to <a href="#metadata-1"><tt>metadata([])</tt></a>.</td></tr><tr><td valign="top"><a href="#metadata-1">metadata/1</a></td><td> 
Return metadata for the given topics.</td></tr><tr><td valign="top"><a href="#offset-1">offset/1</a></td><td>Equivalent to <a href="#offset-2"><tt>offset(-1, Topics)</tt></a>.</td></tr><tr><td valign="top"><a href="#offset-2">offset/2</a></td><td> 
Get offet for the given topics and replicat.</td></tr><tr><td valign="top"><a href="#offset_commit-2">offset_commit/2</a></td><td> 
Offset commit v0.</td></tr><tr><td valign="top"><a href="#offset_commit-4">offset_commit/4</a></td><td> 
Offset commit v1.</td></tr><tr><td valign="top"><a href="#offset_commit-5">offset_commit/5</a></td><td> 
Offset commit v2.</td></tr><tr><td valign="top"><a href="#offset_fetch-1">offset_fetch/1</a></td><td>Equivalent to <a href="#offset_fetch-2"><tt>offset_fetch(ConsumerGroup, [])</tt></a>.</td></tr><tr><td valign="top"><a href="#offset_fetch-2">offset_fetch/2</a></td><td> 
Offset fetch.</td></tr><tr><td valign="top"><a href="#offsets-2">offsets/2</a></td><td>
Return the list of unread offsets for a given topic and consumer group.</td></tr><tr><td valign="top"><a href="#produce-2">produce/2</a></td><td>Equivalent to <a href="#produce-3"><tt>produce(Topic, Message, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#produce-3">produce/3</a></td><td> 
Send a message.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="consumer_metadata-1"></a>

### consumer_metadata/1 ###

<pre><code>
consumer_metadata(ConsumerGroup::binary()) -&gt; {ok, <a href="#type-consumer_metadata">consumer_metadata()</a>}
</code></pre>
<br />


Consumer Metadata Request

For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ConsumerMetadataRequest).

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
fetch(ReplicatID::integer(), TopicName::binary(), Options::<a href="#type-fetch_options">fetch_options()</a>) -&gt; {ok, [<a href="#type-message_set">message_set()</a>]}
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
metadata(Topics::[binary()]) -&gt; {ok, <a href="#type-metadata">metadata()</a>}
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
offset(ReplicatID::integer(), Topics::<a href="#type-topics">topics()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
</code></pre>
<br />


Get offet for the given topics and replicat

Example:

```

 Offset = kafe:offet(-1, [<<"topic1">>, {<<"topic2">>, [{0, -1, 1}, {2, -1, 1}]}]).
```

For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetRequest).

<a name="offset_commit-2"></a>

### offset_commit/2 ###

<pre><code>
offset_commit(ConsumerGroup::binary(), Topics::<a href="#type-offset_commit_option">offset_commit_option()</a>) -&gt; {ok, [<a href="#type-offset_commit_set">offset_commit_set()</a>]}
</code></pre>
<br />


Offset commit v0

For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).

<a name="offset_commit-4"></a>

### offset_commit/4 ###

<pre><code>
offset_commit(ConsumerGroup::binary(), ConsumerGroupGenerationId::integer(), ConsumerId::binary(), Topics::<a href="#type-offset_commit_option_v1">offset_commit_option_v1()</a>) -&gt; {ok, [<a href="#type-offset_commit_set">offset_commit_set()</a>]}
</code></pre>
<br />


Offset commit v1

For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).

<a name="offset_commit-5"></a>

### offset_commit/5 ###

<pre><code>
offset_commit(ConsumerGroup::binary(), ConsumerGroupGenerationId::integer(), ConsumerId::binary(), RetentionTime::integer(), Topics::<a href="#type-offset_commit_option">offset_commit_option()</a>) -&gt; {ok, [<a href="#type-offset_commit_set">offset_commit_set()</a>]}
</code></pre>
<br />


Offset commit v2

For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommitRequest).

<a name="offset_fetch-1"></a>

### offset_fetch/1 ###

<pre><code>
offset_fetch(ConsumerGroup::binary()) -&gt; {ok, [<a href="#type-offset_fetch_set">offset_fetch_set()</a>]}
</code></pre>
<br />

Equivalent to [`offset_fetch(ConsumerGroup, [])`](#offset_fetch-2).

<a name="offset_fetch-2"></a>

### offset_fetch/2 ###

<pre><code>
offset_fetch(ConsumerGroup::binary(), Options::<a href="#type-offset_fetch_options">offset_fetch_options()</a>) -&gt; {ok, [<a href="#type-offset_fetch_set">offset_fetch_set()</a>]}
</code></pre>
<br />


Offset fetch

For more informations, see the [Kafka protocol documentation](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchRequest).

<a name="offsets-2"></a>

### offsets/2 ###

<pre><code>
offsets(TopicName::binary(), ConsumerGroup::binary()) -&gt; [{integer(), integer()}] | error
</code></pre>
<br />

Return the list of unread offsets for a given topic and consumer group

<a name="produce-2"></a>

### produce/2 ###

`produce(Topic, Message) -> any()`

Equivalent to [`produce(Topic, Message, #{})`](#produce-3).

<a name="produce-3"></a>

### produce/3 ###

<pre><code>
produce(Topic::binary(), Message::<a href="#type-message">message()</a>, Options::<a href="#type-produce_options">produce_options()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
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

