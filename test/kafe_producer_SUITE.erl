-module(kafe_producer_SUITE).
-compile([{parse_transform, lager_transform}]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("kafe_ct_common.hrl").

-export([
         init_per_suite/1
         , end_per_suite/1
         , init_per_testcase/2
         , end_per_testcase/2
         , all/0
         , suite/0
        ]).

-export([
         t_produce/1
         , t_produce_no_ack/1
        ]).

suite() ->
   [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(lager),
  kafe_test_cluster:up(),
  {ok, _} = application:ensure_all_started(kafe),
  Config.

end_per_suite(_Config) ->
  application:stop(kafe),
  application:stop(poolgirl),
  ok.

init_per_testcase(_Case, Config) ->
  ?RETRY(
   begin
     {ok, #{
         topics := [#{name := <<"testone">>,
                      partitions := [#{error_code := none,
                                       high_watermark_offset := HWO,
                                       partition := 0}]}]}} =
       kafe:fetch(-1, [{<<"testone">>, 0}], #{}),
     [{offset, HWO}|Config]
   end).

end_per_testcase(_Case, Config) ->
  lists:keydelete(offset, 1, Config).

all() ->
  [F||{F, _A} <- module_info(exports),
      case atom_to_list(F) of
        "t_" ++ _ -> true;
        _ -> false
      end].

t_produce(Config) ->
  {offset, NextOffset} = lists:keyfind(offset, 1, Config),

  {ok, #{throttle_time := 0,
         topics := [#{name := <<"testone">>,
                      partitions := [#{error_code := none,
                                       offset := Offset,
                                       partition := Partition}]}]}} =
  kafe:produce([{<<"testone">>, [{<<"t_produce_key_0">>, <<"t_produce_value_0">>}]}]),
  Offset = NextOffset,

  {ok, #{throttle_time := 0,
         topics := [#{name := <<"testone">>,
                      partitions := [#{error_code := none,
                                       high_watermark_offset := HWO,
                                       messages := [#{attributes := 0,
                                                      crc := _,
                                                      key := <<"t_produce_key_0">>,
                                                      magic_byte := 1,
                                                      offset := NextOffset,
                                                      timestamp := _,
                                                      value := <<"t_produce_value_0">>}],
                                       partition := 0}]}]}} =
  kafe:fetch(-1, [{<<"testone">>, [{Partition, NextOffset}]}], #{}),
  HWO = NextOffset + 1.


t_produce_no_ack(Config) ->
  {offset, NextOffset} = lists:keyfind(offset, 1, Config),

  ok = kafe:produce([{<<"testone">>, [{<<"t_produce_no_ack_key_0">>, <<"t_produce_no_ack_value_0">>}]}], #{required_acks => 0}),

  {ok, #{throttle_time := 0,
         topics := [#{name := <<"testone">>,
                      partitions := [#{error_code := none,
                                       high_watermark_offset := HWO,
                                       messages := [#{attributes := 0,
                                                      crc := _,
                                                      key := <<"t_produce_no_ack_key_0">>,
                                                      magic_byte := 1,
                                                      offset := NextOffset,
                                                      timestamp := _,
                                                      value := <<"t_produce_no_ack_value_0">>}],
                                       partition := 0}]}]}} =
  kafe:fetch(-1, [{<<"testone">>, [{0, NextOffset}]}], #{}),
  HWO = NextOffset + 1.

