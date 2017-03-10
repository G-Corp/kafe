# File: Kafe.Consumer.Subscriber.ex
# This file was generated from kafe_consumer_subscriber.beam
# Using rebar3_elixir (https://github.com/botsunit/rebar3_elixir)
# MODIFY IT AT YOUR OWN RISK AND ONLY IF YOU KNOW WHAT YOU ARE DOING!
defmodule Kafe.Consumer.Subscriber do
  @callback init(any, any, any, any) :: any
  @callback handle_message(any, any) :: any
  def unquote(:"message")(arg1, arg2) do
    :erlang.apply(:"kafe_consumer_subscriber", :"message", [arg1, arg2])
  end
end
