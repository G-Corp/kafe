

# Module kafe #
* [Function Index](#index)
* [Function Details](#functions)

Copyright (c) 2014 Finexkap

A Kafka client un pure Erlang

__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Gregoire Lejeune ([`gl@finexkap.com`](mailto:gl@finexkap.com)).
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#fetch-2">fetch/2</a></td><td>
Fetch messages.</td></tr><tr><td valign="top"><a href="#fetch-3">fetch/3</a></td><td></td></tr><tr><td valign="top"><a href="#metadata-0">metadata/0</a></td><td>
Return kafka metadata.</td></tr><tr><td valign="top"><a href="#metadata-1">metadata/1</a></td><td>
Return metadata for the given topics.</td></tr><tr><td valign="top"><a href="#offset-2">offset/2</a></td><td>
Get offet for the given topics and replicat.</td></tr><tr><td valign="top"><a href="#produce-2">produce/2</a></td><td>
Send a message.</td></tr><tr><td valign="top"><a href="#produce-3">produce/3</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="fetch-2"></a>

### fetch/2 ###

`fetch(ReplicatID, TopicName) -> any()`


Fetch messages
<a name="fetch-3"></a>

### fetch/3 ###

`fetch(ReplicatID, TopicName, Options) -> any()`


<a name="metadata-0"></a>

### metadata/0 ###

`metadata() -> any()`


Return kafka metadata
<a name="metadata-1"></a>

### metadata/1 ###

`metadata(Topics) -> any()`


Return metadata for the given topics
<a name="offset-2"></a>

### offset/2 ###

`offset(ReplicatID, Topics) -> any()`


Get offet for the given topics and replicat
<a name="produce-2"></a>

### produce/2 ###

`produce(Topic, Message) -> any()`


Send a message
<a name="produce-3"></a>

### produce/3 ###

`produce(Topic, Message, Options) -> any()`


