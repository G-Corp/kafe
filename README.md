# kafe

A Kafka client un pure Erlang

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
      Partition = #{error_code => Error, id => ID, isr => [ISR], leader => Leader, replicas => [Replicat]}
        ISR = integer()
        Leader = integer()
        Replicat = integer()
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
      Partition = #{error_code => Error, id => ID, isr => [ISR], leader => Leader, replicas => [Replicat]}
        ISR = integer()
        Leader = integer()
        Replicat = integer()
  ID = integer()
  Error = atom()
```

#### `kafe:offset(Replica, Topics) -> Result`

Types:

```
  Replica = integer()
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

Types:

```
  TopicName = binary()
  Message = binary() | {binary(), binary()}

```

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

## Contributing

1. Fork it ( https://github.com/finexkap/nk_message/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
