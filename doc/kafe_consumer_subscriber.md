

# Module kafe_consumer_subscriber #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

__Behaviours:__ [`gen_server`](gen_server.md).

__This module defines the `kafe_consumer_subscriber` behaviour.__<br /> Required callback functions: `init/4`, `handle_message/2`.

<a name="types"></a>

## Data Types ##




### <a name="type-message">message()</a> ###


<pre><code>
message() = #message{}
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#message-2">message/2</a></td><td> 
Get message attributes.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="message-2"></a>

### message/2 ###

`message(Message, Field) -> any()`


Get message attributes.

Example:

```

 Topic = kafe_consumer_subscriber:message(Message, topic).
```

