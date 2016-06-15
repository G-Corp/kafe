# File: Kafe.ex
# This file was generated from kafe.beam
# Using rebar3_elixir (https://github.com/botsunit/rebar3_elixir)
# MODIFY IT AT YOUR OWN RISK AND ONLY IF YOU KNOW WHAT YOU ARE DOING!
defmodule Kafe do
  def unquote(:"start_link")() do
    :erlang.apply(:"kafe", :"start_link", [])
  end
  def unquote(:"first_broker")() do
    :erlang.apply(:"kafe", :"first_broker", [])
  end
  def unquote(:"release_broker")(arg1) do
    :erlang.apply(:"kafe", :"release_broker", [arg1])
  end
  def unquote(:"broker_id_by_topic_and_partition")(arg1, arg2) do
    :erlang.apply(:"kafe", :"broker_id_by_topic_and_partition", [arg1, arg2])
  end
  def unquote(:"broker_by_name")(arg1) do
    :erlang.apply(:"kafe", :"broker_by_name", [arg1])
  end
  def unquote(:"broker_by_host_and_port")(arg1, arg2) do
    :erlang.apply(:"kafe", :"broker_by_host_and_port", [arg1, arg2])
  end
  def unquote(:"broker_by_id")(arg1) do
    :erlang.apply(:"kafe", :"broker_by_id", [arg1])
  end
  def unquote(:"topics")() do
    :erlang.apply(:"kafe", :"topics", [])
  end
  def unquote(:"partitions")(arg1) do
    :erlang.apply(:"kafe", :"partitions", [arg1])
  end
  def unquote(:"max_offset")(arg1) do
    :erlang.apply(:"kafe", :"max_offset", [arg1])
  end
  def unquote(:"max_offset")(arg1, arg2) do
    :erlang.apply(:"kafe", :"max_offset", [arg1, arg2])
  end
  def unquote(:"partition_for_offset")(arg1, arg2) do
    :erlang.apply(:"kafe", :"partition_for_offset", [arg1, arg2])
  end
  def unquote(:"api_version")() do
    :erlang.apply(:"kafe", :"api_version", [])
  end
  def unquote(:"state")() do
    :erlang.apply(:"kafe", :"state", [])
  end
  def unquote(:"start")() do
    :erlang.apply(:"kafe", :"start", [])
  end
  def unquote(:"brokers")() do
    :erlang.apply(:"kafe", :"brokers", [])
  end
  def unquote(:"metadata")() do
    :erlang.apply(:"kafe", :"metadata", [])
  end
  def unquote(:"metadata")(arg1) do
    :erlang.apply(:"kafe", :"metadata", [arg1])
  end
  def unquote(:"offset")() do
    :erlang.apply(:"kafe", :"offset", [])
  end
  def unquote(:"offset")(arg1) do
    :erlang.apply(:"kafe", :"offset", [arg1])
  end
  def unquote(:"offset")(arg1, arg2) do
    :erlang.apply(:"kafe", :"offset", [arg1, arg2])
  end
  def unquote(:"produce")(arg1, arg2) do
    :erlang.apply(:"kafe", :"produce", [arg1, arg2])
  end
  def unquote(:"produce")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"produce", [arg1, arg2, arg3])
  end
  def unquote(:"default_key_to_partition")(arg1, arg2) do
    :erlang.apply(:"kafe", :"default_key_to_partition", [arg1, arg2])
  end
  def unquote(:"fetch")(arg1) do
    :erlang.apply(:"kafe", :"fetch", [arg1])
  end
  def unquote(:"fetch")(arg1, arg2) do
    :erlang.apply(:"kafe", :"fetch", [arg1, arg2])
  end
  def unquote(:"fetch")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"fetch", [arg1, arg2, arg3])
  end
  def unquote(:"list_groups")() do
    :erlang.apply(:"kafe", :"list_groups", [])
  end
  def unquote(:"list_groups")(arg1) do
    :erlang.apply(:"kafe", :"list_groups", [arg1])
  end
  def unquote(:"group_coordinator")(arg1) do
    :erlang.apply(:"kafe", :"group_coordinator", [arg1])
  end
  def unquote(:"consumer_metadata")(arg1) do
    :erlang.apply(:"kafe", :"consumer_metadata", [arg1])
  end
  def unquote(:"join_group")(arg1) do
    :erlang.apply(:"kafe", :"join_group", [arg1])
  end
  def unquote(:"join_group")(arg1, arg2) do
    :erlang.apply(:"kafe", :"join_group", [arg1, arg2])
  end
  def unquote(:"default_protocol")(arg1, arg2, arg3, arg4) do
    :erlang.apply(:"kafe", :"default_protocol", [arg1, arg2, arg3, arg4])
  end
  def unquote(:"sync_group")(arg1, arg2, arg3, arg4) do
    :erlang.apply(:"kafe", :"sync_group", [arg1, arg2, arg3, arg4])
  end
  def unquote(:"heartbeat")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"heartbeat", [arg1, arg2, arg3])
  end
  def unquote(:"leave_group")(arg1, arg2) do
    :erlang.apply(:"kafe", :"leave_group", [arg1, arg2])
  end
  def unquote(:"describe_group")(arg1) do
    :erlang.apply(:"kafe", :"describe_group", [arg1])
  end
  def unquote(:"offset_commit")(arg1, arg2) do
    :erlang.apply(:"kafe", :"offset_commit", [arg1, arg2])
  end
  def unquote(:"offset_commit")(arg1, arg2, arg3, arg4) do
    :erlang.apply(:"kafe", :"offset_commit", [arg1, arg2, arg3, arg4])
  end
  def unquote(:"offset_commit")(arg1, arg2, arg3, arg4, arg5) do
    :erlang.apply(:"kafe", :"offset_commit", [arg1, arg2, arg3, arg4, arg5])
  end
  def unquote(:"offset_fetch")(arg1) do
    :erlang.apply(:"kafe", :"offset_fetch", [arg1])
  end
  def unquote(:"offset_fetch")(arg1, arg2) do
    :erlang.apply(:"kafe", :"offset_fetch", [arg1, arg2])
  end
  def unquote(:"offsets")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"offsets", [arg1, arg2, arg3])
  end
  def unquote(:"offsets")(arg1, arg2) do
    :erlang.apply(:"kafe", :"offsets", [arg1, arg2])
  end
  def unquote(:"start_consumer")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"start_consumer", [arg1, arg2, arg3])
  end
  def unquote(:"stop_consumer")(arg1) do
    :erlang.apply(:"kafe", :"stop_consumer", [arg1])
  end
  def unquote(:"init")(arg1) do
    :erlang.apply(:"kafe", :"init", [arg1])
  end
  def unquote(:"handle_call")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"handle_call", [arg1, arg2, arg3])
  end
  def unquote(:"handle_cast")(arg1, arg2) do
    :erlang.apply(:"kafe", :"handle_cast", [arg1, arg2])
  end
  def unquote(:"handle_info")(arg1, arg2) do
    :erlang.apply(:"kafe", :"handle_info", [arg1, arg2])
  end
  def unquote(:"terminate")(arg1, arg2) do
    :erlang.apply(:"kafe", :"terminate", [arg1, arg2])
  end
  def unquote(:"code_change")(arg1, arg2, arg3) do
    :erlang.apply(:"kafe", :"code_change", [arg1, arg2, arg3])
  end
end
