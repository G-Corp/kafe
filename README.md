

# A Kafka client for Erlang #

Copyright (c) 2014, 2015 Finexkap, 2015 G-Corp, 2015, 2016 BotsUnit

__Version:__ 1.1.0

__Authors:__ Gregoire Lejeune ([`gregoire.lejeune@finexkap.com`](mailto:gregoire.lejeune@finexkap.com)), Gregoire Lejeune ([`greg@g-corp.io`](mailto:greg@g-corp.io)), Gregoire Lejeune ([`gregoire.lejeune@botsunit.com`](mailto:gregoire.lejeune@botsunit.com)).


### Links ###
* [Apache Kafka](http://kafka.apache.org)
* [Apache Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)



### Create a consumer ###

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

See [`kafe:start_consumer/3`](https://github.com/botsunit/kafe/blob/master/doc/kafe.md#start_consumer-3) for the available `Options`.

In the `consume` function, if you didn't start the consumer with `autocommit` set to `true`, you need to commit manually when you
have finished to treat the message. To do so, use [`kafe_consumer:commit/1`](https://github.com/botsunit/kafe/blob/master/doc/kafe_consumer.md#commit-1) with the `CommitID` as parameter.

When you are done with your consumer, stop it :

```

...
kafe:stop_consumer(my_group),
...

```


### Using with Elixir ###

Elixir' users can use `Kafe` and `Kafe.Consumer` instead of `:kafe` and `:kafe_consumer`.

```

defmodule MyConsumer do
  def consume(commit_id, topic, partition, offset, key, value) do
    # Do something with topic/partition/offset/key/value
    :ok
  end
end

```

```

...
Kafe:start()
...
Kafe:start_consumer(:my_group, &My.Consumer.consume/6, options)
...
Kafe:stop_consumer(:my_group)
...

```


### API Documentation ###

See [documentation](https://github.com/botsunit/kafe/blob/master/doc/doc/)


### Contributing ###
1. Fork it ( https://github.com/botsunit/kafe/fork )
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Commit your changes (`git commit -am 'Add some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create a new Pull Request



### Licence ###

kafe is available for use under the following license, commonly known as the 3-clause (or "modified") BSD license:

Copyright (c) 2014, 2015 Finexkap<br />
Copyright (c) 2015, G-Corp<br />
Copyright (c) 2015, 2016 BotsUnit<br />

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
* The name of the author may not be used to endorse or promote products derived from this software without specific prior written permission.



THIS SOFTWARE IS PROVIDED BY THE AUTHOR `AS IS` AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


## Modules ##


<table width="100%" border="0" summary="list of modules">
<tr><td><a href="https://github.com/botsunit/kafe/blob/master/doc/kafe.md" class="module">kafe</a></td></tr>
<tr><td><a href="https://github.com/botsunit/kafe/blob/master/doc/kafe_consumer.md" class="module">kafe_consumer</a></td></tr></table>

