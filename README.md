# kafe

A Kafka client un pure Erlang

## Links

* [Apache Kafka](http://kafka.apache.org/)
* [Apache Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

## Applications

### `kafe`

```
application:ensure_all_started(kafe).
```

## Module

### `kafe`

#### `kafe:metadata() -> Result`

Types:

```
  Result = {ok, #{brokers => [Broker], topics => [Topic]}}
    Broker = #{host => Host, id => ID, port => Port}
      Host = binary()
      Port = integer()
    Topic = #{error_code = Error, name => Name, partitions => [Partition]}
      Name = binary()
      Partition = #{error_code => Error, id => ID, isr => [ISR], leader => Leader, replicas => [ReplicatID]}
        ISR = integer()
        Leader = integer()
        ReplicatID = integer()
  ID = integer()
  Error = atom()
```

#### `kafe:metadata(TopicNames) -> Result`

Types:

```
  TopicNames = [binary()]
  Result = {ok, #{brokers => [Broker], topics => [Topic]}}
    Broker = #{host => Host, id => ID, port => Port}
      Host = binary()
      Port = integer()
    Topic = #{error_code = Error, name => Name, partitions => [Partition]}
      Name = binary()
      Partition = #{error_code => Error, id => ID, isr => [ISR], leader => Leader, replicas => [ReplicatID]}
        ISR = integer()
        Leader = integer()
        ReplicatID = integer()
  ID = integer()
  Error = atom()
```

#### `kafe:offset(ReplicatID, Topics) -> Result`

Types:

```
  ReplicatID = integer()
  Topics = [TopicName] | [TopicName, [Partition]] 
    Partition = {PartitionNumber, FetchOffset, MaxBytes}
      PartitionNumber = integer()
      FetchOffset = integer()
      MaxBytes = integer()
  Result = {ok,[TopicPartitionInfo]}
    TopicPartitionInfo = #{name => TopicName, partitions => [PartitionInfo]}
      PartitionInfo = #{error_code => Error, id => ID, offsets => [Offset]}
        Error = atom()
        ID = integer()
        Offset = integer()
  TopicName = binary()
```

#### `kafe:produce(TopicName, Message) -> Result`

Same as `kafe:produce(TopicName, Message, #{})`.

#### `kafe:produce(TopicName, Message, Options) -> Result`

Types:

```
  TopicName = binary()
  Message = binary() | {binary(), binary()}
  Options = #{Option => Value}
  Result = {ok, [Topic]}
    Topic = #{name => Name, partitions => [Partition]}
      Name = binary()
      Partition = #{error_code => Error, offset => Offset, partition => Number}
        Error = atom()
        Offset = integer()
        Number = integer()
```

`Option` is any of :

* `timeout :: integer()`
* `required_acks :: integer()`
* `partition :: integer()`

#### `kafe:fetch(ReplicatID, TopicName)`

Same as `kafe:fetch(ReplicatID, TopicName, #{})`

#### `kafe:fetch(ReplicatID, TopicName, Options)`

Types:

```
  ReplicatID = integer() % must alwaus be -1
  TopicName = binary()
  Options = #{Option => Value}
    Option = atom()
    Value = term()
```

`Option` is any of :

* `partition :: integer()`
* `offset :: integer()`
* `max_bytes :: integer()`
* `min_bytes :: integer()`
* `max_wait_time :: integer()`

## Contributing

1. Fork it ( https://github.com/finexkap/nk_message/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
