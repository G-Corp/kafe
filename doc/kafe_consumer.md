

# Module kafe_consumer #
* [Description](#description)


A Kafka client for Erlang.

Copyright (c) 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit

__Introduced in:__ 2014

__Behaviours:__ [`gen_server`](gen_server.md).

__Authors:__ Grégoire Lejeune ([`gregoire.lejeune@botsunit.com`](mailto:gregoire.lejeune@botsunit.com)).

<a name="description"></a>

## Description ##

To create a consumer, use this behaviour :

```

 -module(my_consumer).
 -behaviour(kafe_consumer).

 -export([init/1, consume/3]).

 init(_Args) ->
   ok.

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
