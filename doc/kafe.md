

# Module kafe #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

Copyright (c) 2014 Finexkap

A Kafka client un pure Erlang

__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Gregoire Lejeune ([`gl@finexkap.com`](mailto:gl@finexkap.com)).

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





### <a name="type-offset">offset()</a> ###



<pre><code>
offset() = integer()
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


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#fetch-2">fetch/2</a></td><td>Equivalent to <a href="#fetch-3"><tt>fetch(ReplicatID, TopicName, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#fetch-3">fetch/3</a></td><td> 
Fetch messages.</td></tr><tr><td valign="top"><a href="#metadata-0">metadata/0</a></td><td>
Return kafka metadata.</td></tr><tr><td valign="top"><a href="#metadata-1">metadata/1</a></td><td>
Return metadata for the given topics.</td></tr><tr><td valign="top"><a href="#offset-2">offset/2</a></td><td>
Get offet for the given topics and replicat.</td></tr><tr><td valign="top"><a href="#produce-2">produce/2</a></td><td>Equivalent to <a href="#produce-3"><tt>produce(Topic, Message, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#produce-3">produce/3</a></td><td>
Send a message.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="fetch-2"></a>

### fetch/2 ###


<pre><code>
fetch(ReplicatID::<a href="#type-replicat">replicat()</a>, TopicName::<a href="#type-topic_name">topic_name()</a>) -&gt; {ok, [<a href="#type-message_set">message_set()</a>]}
</code></pre>
<br />

Equivalent to [`fetch(ReplicatID, TopicName, #{})`](#fetch-3).
<a name="fetch-3"></a>

### fetch/3 ###


<pre><code>
fetch(ReplicatID::<a href="#type-replicat">replicat()</a>, TopicName::<a href="#type-topic_name">topic_name()</a>, Options::<a href="#type-fetch_options">fetch_options()</a>) -&gt; {ok, [<a href="#type-message_set">message_set()</a>]}
</code></pre>
<br />


 
Fetch messages


> ReplicatID must *always* be -1
<a name="metadata-0"></a>

### metadata/0 ###


<pre><code>
metadata() -&gt; {ok, <a href="#type-metadata">metadata()</a>}
</code></pre>
<br />


Return kafka metadata
<a name="metadata-1"></a>

### metadata/1 ###


<pre><code>
metadata(Topics::<a href="#type-topics">topics()</a>) -&gt; {ok, <a href="#type-metadata">metadata()</a>}
</code></pre>
<br />


Return metadata for the given topics
<a name="offset-2"></a>

### offset/2 ###


<pre><code>
offset(ReplicatID::<a href="#type-replicat">replicat()</a>, Topics::<a href="#type-topics">topics()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
</code></pre>
<br />


Get offet for the given topics and replicat
<a name="produce-2"></a>

### produce/2 ###


<pre><code>
produce(Topic::<a href="#type-topic_name">topic_name()</a>, Message::<a href="#type-message">message()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
</code></pre>
<br />

Equivalent to [`produce(Topic, Message, #{})`](#produce-3).
<a name="produce-3"></a>

### produce/3 ###


<pre><code>
produce(Topic::<a href="#type-topic_name">topic_name()</a>, Message::<a href="#type-message">message()</a>, Options::<a href="#type-produce_options">produce_options()</a>) -&gt; {ok, [<a href="#type-topic_partition_info">topic_partition_info()</a>]}
</code></pre>
<br />


Send a message
