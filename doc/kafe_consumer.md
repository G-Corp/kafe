

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

To create a consumer, create a function with 6 parameters :

```

 -module(my_consumer).

 -export([consume/6]).

 consume(GroupID, Topic, Partition, Offset, Key, Value) ->
   % Do something with Topic/Partition/Offset/Key/Value
   ok.
```

The `consume` function must return `ok` if the message was treated, or `{error, term()}` on error.

Then start a new consumer :

```

 ...
 kafe:start(),
 ...
 kafe:start_consumer(my_group, fun my_consumer:consume/6, Options),
 ...
```

See [`kafe:start_consumer/3`](kafe.md#start_consumer-3) for the available `Options`.

In the `consume` function, if you didn't start the consumer in autocommit mode (using `before_processing | after_processing` in the `commit` options),
you need to commit manually when you have finished to treat the message. To do so, use [`kafe_consumer:commit/4`](kafe_consumer.md#commit-4).

When you are done with your consumer, stop it :

```

 ...
 kafe:stop_consumer(my_group),
 ...
```

You can also use a `kafe_consumer_subscriber` behaviour instead of a function :

```

 -module(my_consumer).
 -behaviour(kafe_consumer_subscriber).
 -include_lib("kafe/include/kafe_consumer.hrl").

 -export([init/4, handle_message/2]).

 -record(state, {
                }).

 init(Group, Topic, Partition, Args) ->
   % Do something with Group, Topic, Partition, Args
   {ok, #state{}}.

 handle_message(Message, State) ->
   % Do something with Message
   % And update your State (if needed)
   {ok, NewState}.
```

Then start a new consumer :

```

 ...
 kafe:start().
 ...
 kafe:start_consumer(my_group, {my_consumer, Args}, Options).
 % Or
 kafe:start_consumer(my_group, my_consumer, Options).
 ...
```

To commit a message (if you need to), use [`kafe_consumer:commit/4`](kafe_consumer.md#commit-4).

__Internal :__

```

                                                                 one per consumer group
                                                       +--------------------^--------------------+
                                                       |                                         |

                                                                          +--> kafe_consumer_srv +-----------------+
                  +--> kafe_consumer_sup +------so4o---> kafe_consumer +--+                                        |
                  |                                                       +--> kafe_consumer_statem                |
                  +--> kafe_consumer_group_sup +--+                                                                m
 kafe_sup +--o4o--+                               |                                                                o
                  +--> kafe_rr                    s                                                                n
                  |                               o                                                                |
                  +--> kafe                       4                                                                |
                                                  o                                                                |
                                                  |                                  +--> kafe_consumer_fetcher <--+
                                                  +--> kafe_consumer_tp_group_sup +--+
                                                                                     +--> kafe_consumer_committer
                                                                                     |
                                                                                     +--> kafe_consumer_subscriber

                                                     |                                                            |
                                                     +-------------------------------v----------------------------+
                                                                           one/{topic,partition}

 (o4o = one_for_one)
 (so4o = simple_one_for_one)
 (mon = monitor)
```
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#commit-1">commit/1</a></td><td>
Commit the offset for the given message.</td></tr><tr><td valign="top"><a href="#commit-4">commit/4</a></td><td>
Commit the <tt>Offset</tt> for the given <tt>GroupID</tt>, <tt>Topic</tt> and <tt>Partition</tt>.</td></tr><tr><td valign="top"><a href="#coordinator-1">coordinator/1</a></td><td>
Return the consumer group coordinator.</td></tr><tr><td valign="top"><a href="#describe-1">describe/1</a></td><td>
Return consumer group descrition.</td></tr><tr><td valign="top"><a href="#generation_id-1">generation_id/1</a></td><td>
Return the consumer group generation ID.</td></tr><tr><td valign="top"><a href="#list-0">list/0</a></td><td>Equivalent to <a href="kafe.md#consumer_groups-0"><tt>kafe:consumer_groups()</tt></a>.</td></tr><tr><td valign="top"><a href="#member_id-1">member_id/1</a></td><td>
Return the consumer group member ID.</td></tr><tr><td valign="top"><a href="#pending_commits-1">pending_commits/1</a></td><td>
Return the number of pending commits for the given consumer group.</td></tr><tr><td valign="top"><a href="#pending_commits-3">pending_commits/3</a></td><td>
Return the number or pending commits for the given consumer group, topic and partition.</td></tr><tr><td valign="top"><a href="#remove_commits-1">remove_commits/1</a></td><td>
Remove pending commits for the given consumer group.</td></tr><tr><td valign="top"><a href="#remove_commits-3">remove_commits/3</a></td><td>
Remove pending commits for the given consumer group, topic and partition.</td></tr><tr><td valign="top"><a href="#start-3">start/3</a></td><td>Equivalent to <a href="kafe.md#start_consumer-3"><tt>kafe:start_consumer(GroupID, Callback, Options)</tt></a>.</td></tr><tr><td valign="top"><a href="#stop-1">stop/1</a></td><td>Equivalent to <a href="kafe.md#stop_consumer-1"><tt>kafe:stop_consumer(GroupID)</tt></a>.</td></tr><tr><td valign="top"><a href="#topics-1">topics/1</a></td><td>
Return the list of {topic, partition} for the consumer group.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="commit-1"></a>

### commit/1 ###

<pre><code>
commit(Message::<a href="kafe_consumer_subscriber.md#type-message">kafe_consumer_subscriber:message()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

Commit the offset for the given message

<a name="commit-4"></a>

### commit/4 ###

<pre><code>
commit(GroupID::binary(), Topic::binary(), Partition::integer(), Offset::integer()) -&gt; ok | {error, term()}
</code></pre>
<br />

Commit the `Offset` for the given `GroupID`, `Topic` and `Partition`.

<a name="coordinator-1"></a>

### coordinator/1 ###

<pre><code>
coordinator(GroupID::binary()) -&gt; atom() | undefined
</code></pre>
<br />

Return the consumer group coordinator

<a name="describe-1"></a>

### describe/1 ###

<pre><code>
describe(GroupID::binary()) -&gt; {ok, <a href="kafe.md#type-describe_group">kafe:describe_group()</a>} | {error, term()}
</code></pre>
<br />

Return consumer group descrition

<a name="generation_id-1"></a>

### generation_id/1 ###

<pre><code>
generation_id(GroupID::binary()) -&gt; integer()
</code></pre>
<br />

Return the consumer group generation ID

<a name="list-0"></a>

### list/0 ###

`list() -> any()`

Equivalent to [`kafe:consumer_groups()`](kafe.md#consumer_groups-0).

<a name="member_id-1"></a>

### member_id/1 ###

<pre><code>
member_id(GroupID::binary()) -&gt; binary()
</code></pre>
<br />

Return the consumer group member ID

<a name="pending_commits-1"></a>

### pending_commits/1 ###

<pre><code>
pending_commits(GroupID::binary()) -&gt; integer()
</code></pre>
<br />

Return the number of pending commits for the given consumer group

<a name="pending_commits-3"></a>

### pending_commits/3 ###

<pre><code>
pending_commits(GroupID::binary(), Topic::binary(), Partition::integer()) -&gt; integer()
</code></pre>
<br />

Return the number or pending commits for the given consumer group, topic and partition.

<a name="remove_commits-1"></a>

### remove_commits/1 ###

<pre><code>
remove_commits(GroupID::binary()) -&gt; ok
</code></pre>
<br />

Remove pending commits for the given consumer group

<a name="remove_commits-3"></a>

### remove_commits/3 ###

<pre><code>
remove_commits(GroupID::binary(), Topic::binary(), Partition::integer()) -&gt; ok | {error, Reason::term()}
</code></pre>
<br />

Remove pending commits for the given consumer group, topic and partition.

<a name="start-3"></a>

### start/3 ###

`start(GroupID, Callback, Options) -> any()`

Equivalent to [`kafe:start_consumer(GroupID, Callback, Options)`](kafe.md#start_consumer-3).

<a name="stop-1"></a>

### stop/1 ###

`stop(GroupID) -> any()`

Equivalent to [`kafe:stop_consumer(GroupID)`](kafe.md#stop_consumer-1).

<a name="topics-1"></a>

### topics/1 ###

<pre><code>
topics(GroupID::binary()) -&gt; [{Topic::binary(), Partition::integer()}]
</code></pre>
<br />

Return the list of {topic, partition} for the consumer group

