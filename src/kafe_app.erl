% @hidden
-module(kafe_app).
-compile([{parse_transform, lager_transform}]).

-behaviour(application).

%% Application callbacks
-export([start/2, prep_stop/1, stop/1]).

start(_StartType, _StartArgs) ->
  kafe_sup:start_link().

prep_stop(State) ->
  kafe:stop_brokers(),
  State.

stop(_State) ->
  ok.

