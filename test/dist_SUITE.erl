-module(dist_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
         all/0
         , suite/0
         , init_per_suite/1
         , end_per_suite/1
         , init_per_testcase/2
         , end_per_testcase/2
        ]).

-export([
         simple_test/1
         , simple_test_two/1
        ]).

all() ->
	[simple_test, simple_test_two].

suite() ->
  [].

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  {save_config, Config}.

init_per_testcase(simple_test, Config) ->
  % ct:print("init_per_suite = ~p", [Config]),
  {ok, _} = bucos:run("docker-compose up -d"),
  Config;
init_per_testcase(simple_test_two, Config) ->
  % ct:print("init_per_suite = ~p", [Config]),
  {ok, _} = bucos:run("docker-compose up -d"),
  Config.

end_per_testcase(simple_test, Config) ->
  {ok, _} = bucos:run("docker-compose kill"),
  {ok, _} = bucos:run("docker-compose rm -vf"),
  {save_config, Config};
end_per_testcase(simple_test_two, Config) ->
  {ok, _} = bucos:run("docker-compose kill"),
  {ok, _} = bucos:run("docker-compose rm -vf"),
  {save_config, Config}.

% -- simple_test

simple_test(Config) ->
  A = 2,
  1.0 = 2/A,
  {save_config, Config}.

% -- simple_test_two

simple_test_two(Config) ->
  A = 0,
  1.0 = 2/A,
  {save_config, Config}.



% docker-compose run --rm tools kafka-topics --describe --zookeeper zookeeper:2181
% docker-compose run --rm tools kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 2 --partitions 3 --topic testone

