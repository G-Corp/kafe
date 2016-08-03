# File: Kafe.Consumer.ex
# This file was generated from kafe_consumer.beam
# Using rebar3_elixir (https://github.com/botsunit/rebar3_elixir)
# MODIFY IT AT YOUR OWN RISK AND ONLY IF YOU KNOW WHAT YOU ARE DOING!
defmodule Kafe.Consumer do
  def unquote(:"start")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe_consumer", :"start", [arg1, arg2, arg3])
  end
  def unquote(:"stop")(arg1) do
    :erlang.apply(:"kafe_consumer", :"stop", [arg1])
  end
  def unquote(:"describe")(arg1) do
    :erlang.apply(:"kafe_consumer", :"describe", [arg1])
  end
  def unquote(:"commit")(arg1) do
    :erlang.apply(:"kafe_consumer", :"commit", [arg1])
  end
  def unquote(:"commit")(arg1, arg2) do
    :erlang.apply(:"kafe_consumer", :"commit", [arg1, arg2])
  end
  def unquote(:"remove_commits")(arg1) do
    :erlang.apply(:"kafe_consumer", :"remove_commits", [arg1])
  end
  def unquote(:"remove_commit")(arg1) do
    :erlang.apply(:"kafe_consumer", :"remove_commit", [arg1])
  end
  def unquote(:"pending_commits")(arg1) do
    :erlang.apply(:"kafe_consumer", :"pending_commits", [arg1])
  end
  def unquote(:"pending_commits")(arg1, arg2) do
    :erlang.apply(:"kafe_consumer", :"pending_commits", [arg1, arg2])
  end
  def unquote(:"member_id")(arg1) do
    :erlang.apply(:"kafe_consumer", :"member_id", [arg1])
  end
  def unquote(:"generation_id")(arg1) do
    :erlang.apply(:"kafe_consumer", :"generation_id", [arg1])
  end
  def unquote(:"topics")(arg1) do
    :erlang.apply(:"kafe_consumer", :"topics", [arg1])
  end
end
