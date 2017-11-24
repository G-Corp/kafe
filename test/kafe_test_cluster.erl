-module(kafe_test_cluster).
-compile([{parse_transform, lager_transform}]).
-behavior(supervisor).

-export([
         up/0,
         up/1,
         up1/1,
         down/1,
         down1/1,
         brokers/0
        ]).

% CTH exports
-export([
         id/1,
         init/2
        ]).

% supervisor exports
-export([
         start_link/0,
         init/1
        ]).

% Application callbacks
-export([
         start/2,
         stop/1
        ]).

-define(BROKERS, ["kafka1", "kafka2", "kafka3"]).

id(_Opts) ->
  ?MODULE.

init(_Id, _Opts) ->
  lager:info("Starting kafe_test_cluster"),
  io:format("Starting kafe_test_cluster~n", []),
  {ok, _Apps} = application:ensure_all_started(?MODULE),
  io:format("Started kafe_test_cluster~n", []),
  {ok, #{}}.

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [#{}]).

start(_StartType, _StartArgs) ->
  io:format("Starting kafe_test_cluster app~n", []),
  ?MODULE:start_link().

stop(_State) ->
  ok.

init([#{}]) ->
  % Start from known state
  exec:run("docker-compose stop", [sync]),
  {ok, {
     #{strategy => simple_one_for_one},
     [
      #{id => broker,
        start => {kafe_test_cluster_broker, start_link, []},
        restart => temporary,
        shutdown => 60000,
        type => worker}
     ]
    }}.

up() ->
  up(?BROKERS).

up(Brokers) ->
  lager:info("Starting brokers: ~p", [Brokers]),
  lists:foreach(fun up1/1, Brokers).

down(Brokers) ->
  lager:info("Stopping brokers: ~p", [Brokers]),
  lists:foreach(fun down1/1, Brokers).

brokers() ->
  Children = supervisor:which_children(?MODULE),
  lists:map(fun ({_, Pid, _, _}) ->
                case sys:get_state(Pid) of
                  #{name := BrokerName} -> { BrokerName, Pid };
                  _ -> false
                end
            end, Children).

find_child(BrokerName) ->
  case lists:keyfind(BrokerName, 1, brokers()) of
    {_, Pid} -> Pid;
    _ -> undefined
  end.

up1(BrokerName) ->
  case find_child(BrokerName) of
    undefined ->
      {ok, Pid} = supervisor:start_child(?MODULE, [BrokerName]),
      ok = kafe_test_cluster_broker:wait_for_start(Pid),
      {ok, Pid};
    Pid -> {ok, Pid}
  end.

down1(BrokerName) ->
  case find_child(BrokerName) of
    undefined -> ok;
    Pid -> kafe_test_cluster_broker:down(Pid)
  end.
