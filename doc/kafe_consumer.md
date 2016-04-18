

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

To create a consumer, use this behaviour :

```

 -module(my_consumer).
 -behaviour(kafe_consumer).

 -export([init/1, consume/3]).

 init(Args) ->
   {ok, Args}.

 consume(Offset, Key, Value) ->
   % Do something with Offset/Key/Value
   ok.
```

Then start a new consumer :

```

 ...
 kafe:start(),
 ...
 kafe:start_consumer(my_group, my_consumer, Options),
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


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#start_link-2">start_link/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`

<a name="start_link-2"></a>

### start_link/2 ###

`start_link(GroupId, Options) -> any()`

