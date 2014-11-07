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
```

#### `kafe:metadata(TopicName) -> Result`

Types:

```
```

#### `kafe:offset(Replica, Topics) -> Result`

Types:

```
```

#### `kafe:produce(TopicName, Message) -> Result`

Types:

```
```

#### `kafe:produce(TopicName, Message, Options) -> Result`

Types:

```
```

## Contributing

1. Fork it ( https://github.com/finexkap/nk_message/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
