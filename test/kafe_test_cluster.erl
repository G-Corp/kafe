-module(kafe_test_cluster).
-compile([{parse_transform, lager_transform}]).

-export([
         up/0,
         up/1,
         down/1
        ]).

compose(Cmd) ->
  {ok, _} = bucos:run(["docker-compose " ++ Cmd], [stdout_on_error, {timeout, 30000}]).

up() ->
  up([]).

up(Brokers) ->
  compose(lists:flatten(["up -d ", lists:join(" ", Brokers)])).

down(Brokers) ->
  compose(lists:flatten(["stop ", lists:join(" ", Brokers)])).
