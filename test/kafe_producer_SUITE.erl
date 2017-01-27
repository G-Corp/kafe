-module(kafe_producer_SUITE).

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

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
   [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  application:ensure_all_started(kafe),
  Config.

end_per_suite(_Config) ->
  application:stop(kafe),
  ok.

init_per_testcase(_Case, Config) ->
  {ok, #{throttle_time := 0,
         topics := [#{name := <<"testone">>,
                      partitions := [#{error_code := none,
                                       high_watermark_offset := HWO,
                                       partition := 0}]}]}} =
  kafe:fetch(<<"testone">>, #{partition => 0}),
  [{offset, HWO}|Config].

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
                                                      magic_byte := 0,
                                                      offset := NextOffset,
                                                      value := <<"t_produce_value_0">>}],
                                       partition := 0}]}]}} =
  kafe:fetch(<<"testone">>, #{partition => Partition, offset => NextOffset}),
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
                                                      magic_byte := 0,
                                                      offset := NextOffset,
                                                      value := <<"t_produce_no_ack_value_0">>}],
                                       partition := 0}]}]}} =
  kafe:fetch(<<"testone">>, #{partition => 0,
                              offset => NextOffset}),
  HWO = NextOffset + 1.

