

# Module kafe #
* [Function Index](#index)
* [Function Details](#functions)

Copyright (c) 2014-2015 Finexkap

A Kafka client in pure Erlang

__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ [`Gregoire Lejeune (gl@finexkap.com)`](mailto:Gregoire Lejeune (gl@finexkap.com)).
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


ReplicatID must *always* be -1
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
