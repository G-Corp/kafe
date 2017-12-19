-module(kafe_test_cluster).
-compile([{parse_transform, lager_transform}]).
-behavior(supervisor).

% CTH callbacks
-export([
         id/1,
         init/2,
         pre_init_per_suite/3,
         post_end_per_suite/4,
         pre_init_per_testcase/3,
         post_end_per_testcase/4
        ]).

% Supervisor API and callbacks
-export([
         start_link/0,
         init/1
        ]).

% Application callbacks
-export([
         start/2,
         stop/1
        ]).

% API
-export([
         up/0,
         up/1,
         up1/1,
         down/1,
         down1/1,
         brokers/0
        ]).

-define(BROKERS, ["kafka1", "kafka2", "kafka3"]).

% CTH callbacks

id(_Opts) ->
  ?MODULE.

init(_Id, _Opts) ->
  {ok, _} = application:ensure_all_started(lager),
  lager:info("Starting kafe_test_cluster CTH"),
  {ok, _Apps} = application:ensure_all_started(?MODULE),
  {ok, #{}}.

pre_init_per_suite(Suite, Config, State) ->
  lager:info("pre_init_per_suite ~p", [Suite]),
  {Config, State}.

post_end_per_suite(Suite, _Config, Return, State) ->
  lager:info("post_end_per_suite ~p: ~p", [Suite, Return]),
  {Return, State}.

pre_init_per_testcase(TC, Config, State) ->
  lager:info("pre_init_per_testcase ~p", [TC]),
  {Config, State}.

post_end_per_testcase(TC, _Config, Error, State) ->
  lager:info("post_end_per_testcase ~p: ~p", [TC, Error]),
  {Error, State}.

% Supervisor API and callbacks

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, [#{}]).

init([#{}]) ->
  % Start from known state
  lager:info("Starting kafe_test_cluster supervisor"),
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

% Application callbacks

start(_StartType, _StartArgs) ->
  lager:info("Starting kafe_test_cluster app"),
  ?MODULE:start_link().

stop(_State) ->
  ok.

% API

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
                % HACK
                case (catch sys:get_state(Pid)) of
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
      lager:info("Starting broker ~s", [BrokerName]),
      {ok, Pid} = supervisor:start_child(?MODULE, [BrokerName]),
      ok = kafe_test_cluster_broker:wait_for_start(Pid),
      {ok, Pid};
    Pid ->
      lager:info("Broker ~s is already up", [BrokerName]),
      {ok, Pid}
  end.

down1(BrokerName) ->
  case find_child(BrokerName) of
    undefined ->
      lager:info("Broker ~s is already down", [BrokerName]),
      ok;
    Pid ->
      lager:info("Stopping broker ~s", [BrokerName]),
      kafe_test_cluster_broker:down(Pid)
  end.
