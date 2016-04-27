

# Module kafe_consumer #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


A Kafka client for Erlang.

Copyright (c) 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit

__Introduced in:__ 2014

__Behaviours:__ [`supervisor`](supervisor.md).

__Authors:__ Grégoire Lejeune ([`gregoire.lejeune@botsunit.com`](mailto:gregoire.lejeune@botsunit.com)).

<a name="description"></a>

## Description ##

To create a consumer, create a function with 5 parameters :

```

 -module(my_consumer).
 -behaviour(kafe_consumer).

 -export([consume/5]).

 consume(Topic, Partition, Offset, Key, Value) ->
   % Do something with Topic/Partition/Offset/Key/Value
   ok.
```

Then start a new consumer :

```

 ...
 kafe:start(),
 ...
 kafe:start_consumer(my_group, fun my_consumer:consume/5, Options),
 ...
```

When you are done with your consumer, stop it :

```

 ...
 kafe:stop_consumer(my_group),
 ...
```
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#describe-1">describe/1</a></td><td>
Return consumer group descrition.</td></tr><tr><td valign="top"><a href="#generation_id-1">generation_id/1</a></td><td>
Return the <tt>generation_id</tt> of the consumer.</td></tr><tr><td valign="top"><a href="#member_id-1">member_id/1</a></td><td>
Return the <tt>member_id</tt> of the consumer.</td></tr><tr><td valign="top"><a href="#start-3">start/3</a></td><td>Equivalent to <a href="kafe.md#start_consumer-3"><tt>kafe:start_consumer(GroupId, Callback, Options)</tt></a>.</td></tr><tr><td valign="top"><a href="#stop-1">stop/1</a></td><td>Equivalent to <a href="kafe.md#stop_consumer-1"><tt>kafe:stop_consumer(GroupId)</tt></a>.</td></tr><tr><td valign="top"><a href="#topics-1">topics/1</a></td><td>
Return the topics (and partitions) of the consumer.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="describe-1"></a>

### describe/1 ###

<pre><code>
describe(GroupId::atom()) -&gt; {ok, <a href="kafe.md#type-describe_group">kafe:describe_group()</a>} | {error, term()}
</code></pre>
<br />

Return consumer group descrition

<a name="generation_id-1"></a>

### generation_id/1 ###

`generation_id(GroupId) -> any()`

Return the `generation_id` of the consumer

<a name="member_id-1"></a>

### member_id/1 ###

`member_id(GroupId) -> any()`

Return the `member_id` of the consumer

<a name="start-3"></a>

### start/3 ###

`start(GroupId, Callback, Options) -> any()`

Equivalent to [`kafe:start_consumer(GroupId, Callback, Options)`](kafe.md#start_consumer-3).

<a name="stop-1"></a>

### stop/1 ###

`stop(GroupId) -> any()`

Equivalent to [`kafe:stop_consumer(GroupId)`](kafe.md#stop_consumer-1).

<a name="topics-1"></a>

### topics/1 ###

`topics(GroupId) -> any()`

Return the topics (and partitions) of the consumer

