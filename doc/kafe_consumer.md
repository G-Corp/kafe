

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

 consume(CommitID, Topic, Partition, Offset, Key, Value) ->
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

In the `consume` function, if you didn't start the consumer with `autocommit` set to `true`, you need to commit manually when you
have finished to treat the message. To do so, use [`kafe_consumer:commit/1`](kafe_consumer.md#commit-1) with the `CommitID` as parameter.

When you are done with your consumer, stop it :

```

 ...
 kafe:stop_consumer(my_group),
 ...
```

__Internal :__

```

                                                                    one per consumer group
                                                          +--------------------^--------------------+
                                                          |                                         |

                                                                             +--> kafe_consumer_srv +--+
                  +--> kafe_consumer_sup +------so4o------> kafe_consumer +--+                         |
                  |                                                          +--> kafe_consumer_fsm    |
                  +--> kafe_consumer_fetcher_sup +--+                                                  m
 kafe_sup +--o4o--+                                 |                                                  o
                  +--> kafe_rr                      s                                                  n
                  |                                 o                                                  |
                  +--> kafe                         4  +--> kafe_consumer_fetcher <--------------------+
                                                    o  |                                               |
                                                    |  +--> kafe_consumer_fetcher <--------------------+
                                                    +--+                                               |
                                                       +--> kafe_consumer_fetcher <--------------------+
                                                       |                                               .
                                                       +--> ...                                        .
                                                                                                       .
                                                          |                       |
                                                          +-----------v-----------+
                                                            one/{topic,partition}

 (o4o = one_for_one)
 (so4o = simple_one_for_one)
 (mon = monitor)
```
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#commit-1">commit/1</a></td><td>Equivalent to <a href="#commit-2"><tt>commit(GroupCommitIdentifier, #{})</tt></a>.</td></tr><tr><td valign="top"><a href="#commit-2">commit/2</a></td><td>
Commit the offset (in Kafka) for the given <tt>GroupCommitIdentifier</tt> received in the <tt>Callback</tt> specified when starting the
consumer group (see <a href="kafe.md#start_consumer-3"><code>kafe:start_consumer/3</code></a></td></tr><tr><td valign="top"><a href="#describe-1">describe/1</a></td><td>
Return consumer group descrition.</td></tr><tr><td valign="top"><a href="#generation_id-1">generation_id/1</a></td><td>
Return the <tt>generation_id</tt> of the consumer.</td></tr><tr><td valign="top"><a href="#member_id-1">member_id/1</a></td><td>
Return the <tt>member_id</tt> of the consumer.</td></tr><tr><td valign="top"><a href="#pending_commits-1">pending_commits/1</a></td><td>
Return the list of all pending commits for the given group.</td></tr><tr><td valign="top"><a href="#pending_commits-2">pending_commits/2</a></td><td>
Return the list of pending commits for the given topics (and partitions) for the given group.</td></tr><tr><td valign="top"><a href="#remove_commit-1">remove_commit/1</a></td><td>
Remove the given commit.</td></tr><tr><td valign="top"><a href="#remove_commits-1">remove_commits/1</a></td><td>
Remove all pending commits for the given group.</td></tr><tr><td valign="top"><a href="#start-3">start/3</a></td><td>Equivalent to <a href="kafe.md#start_consumer-3"><tt>kafe:start_consumer(GroupID, Callback, Options)</tt></a>.</td></tr><tr><td valign="top"><a href="#stop-1">stop/1</a></td><td>Equivalent to <a href="kafe.md#stop_consumer-1"><tt>kafe:stop_consumer(GroupID)</tt></a>.</td></tr><tr><td valign="top"><a href="#topics-1">topics/1</a></td><td>
Return the topics (and partitions) of the consumer.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="commit-1"></a>

### commit/1 ###

<pre><code>
commit(GroupCommitIdentifier::<a href="kafe.md#type-group_commit_identifier">kafe:group_commit_identifier()</a>) -&gt; ok | {error, term()} | delayed
</code></pre>
<br />

Equivalent to [`commit(GroupCommitIdentifier, #{})`](#commit-2).

<a name="commit-2"></a>

### commit/2 ###

<pre><code>
commit(GroupCommitIdentifier::<a href="kafe.md#type-group_commit_identifier">kafe:group_commit_identifier()</a>, Options::#{}) -&gt; ok | {error, term()} | delayed
</code></pre>
<br />

Commit the offset (in Kafka) for the given `GroupCommitIdentifier` received in the `Callback` specified when starting the
consumer group (see [`kafe:start_consumer/3`](kafe.md#start_consumer-3)

If the `GroupCommitIdentifier` is not the lowerest offset to commit in the group :

* If the consumer was created with `allow_unordered_commit`, the commit is delayed

* Otherwise this function return `{error, cant_commit}`


Available options:

* `retry :: integer()` : max retry (default 0).

* `delay :: integer()` : Time (in ms) between each retry.


<a name="describe-1"></a>

### describe/1 ###

<pre><code>
describe(GroupPIDOrID::atom() | pid() | binary()) -&gt; {ok, <a href="kafe.md#type-describe_group">kafe:describe_group()</a>} | {error, term()}
</code></pre>
<br />

Return consumer group descrition

<a name="generation_id-1"></a>

### generation_id/1 ###

<pre><code>
generation_id(GroupPIDOrID::atom() | pid() | binary()) -&gt; integer()
</code></pre>
<br />

Return the `generation_id` of the consumer

<a name="member_id-1"></a>

### member_id/1 ###

<pre><code>
member_id(GroupPIDOrID::atom() | pid() | binary()) -&gt; binary()
</code></pre>
<br />

Return the `member_id` of the consumer

<a name="pending_commits-1"></a>

### pending_commits/1 ###

<pre><code>
pending_commits(GroupPIDOrID::atom() | pid() | binary()) -&gt; [<a href="kafe.md#type-group_commit_identifier">kafe:group_commit_identifier()</a>]
</code></pre>
<br />

Return the list of all pending commits for the given group.

<a name="pending_commits-2"></a>

### pending_commits/2 ###

<pre><code>
pending_commits(GroupPIDOrID::atom() | pid() | binary(), Topics::[binary() | {binary(), [integer()]}]) -&gt; [<a href="kafe.md#type-group_commit_identifier">kafe:group_commit_identifier()</a>]
</code></pre>
<br />

Return the list of pending commits for the given topics (and partitions) for the given group.

<a name="remove_commit-1"></a>

### remove_commit/1 ###

<pre><code>
remove_commit(GroupCommitIdentifier::<a href="kafe.md#type-group_commit_identifier">kafe:group_commit_identifier()</a>) -&gt; ok | {error, term()}
</code></pre>
<br />

Remove the given commit

<a name="remove_commits-1"></a>

### remove_commits/1 ###

<pre><code>
remove_commits(GroupPIDOrID::atom() | pid() | binary()) -&gt; ok
</code></pre>
<br />

Remove all pending commits for the given group

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
topics(GroupPIDOrID::atom() | pid() | binary()) -&gt; [{binary(), [integer()]}]
</code></pre>
<br />

Return the topics (and partitions) of the consumer

