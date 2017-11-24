-module(kafe_test_cluster_broker).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-export([start_link/1]).

-export([down/1, wait_for_start/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2, code_change/3]).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

down(Pid) ->
  gen_server:call(Pid, stop, 60000).

wait_for_start(Pid) ->
  gen_server:call(Pid, wait_for_start, 60000).

init(BrokerName) ->
  State = up(#{name => BrokerName}),
  {ok, State}.

handle_cast(_Cast, State) ->
  {noreply, State}.

handle_call(wait_for_start, _From, #{waiting_for_start := _} = State) ->
  {reply, {error, already_starting}, State};
handle_call(wait_for_start, From, #{os_pid := _OsPid, name := BrokerName} = State) ->
  {noreply, State#{waiting_for_start => From}};

handle_call(stop, _From, #{waiting_for_stop := _} = State) ->
  {reply, {error, already_stopping}, State};
handle_call(stop, From, #{os_pid := _OsPid, name := BrokerName} = State) ->
  exec:run("docker-compose exec " ++ BrokerName ++ " kafka-server-stop", [sync]),
  {noreply, State#{waiting_for_stop => From}}.

handle_info({Channel, _OsPid, Data}, State) when Channel == stdout; Channel == stderr ->
  Lines = binary:split(Data, [<<"\r">>, <<"\n">>], [global, trim_all]),
  State1 = lists:foldl(fun process_line/2, State, [{Channel, Line} || Line <- Lines]),
  {noreply, State1};

handle_info({'DOWN', OsPid, process, _Pid, Reason}, State) ->
  lager:warning("OS process ~p exited for reason ~p", [OsPid, Reason]),
  case State of
    #{waiting_for_stop := From} -> gen_server:reply(From, {ok, Reason});
    _ -> ok
  end,
  {stop, normal, State}.

process_line({Channel, Line}, State) ->
  lager:debug("~p: ~ts", [Channel, Line]),
  case binary:match(Line, <<"started (kafka.server.KafkaServer)">>) of
    {_, _} ->
      case maps:take(waiting_for_start, State) of
        {From, State1} ->
          gen_server:reply(From, ok),
          State1;
        _ -> State
      end;
    nomatch -> State
  end.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

run(Cmd, State) ->
  {ok, _Pid, OsPid} = exec:run(Cmd, [monitor, stdout, stderr, {kill_timeout, 60000}]),
  State#{os_pid => OsPid}.

compose(Cmd, State) ->
  run("docker-compose --no-ansi " ++ Cmd, State).

up(#{name := BrokerName} = State) ->
  lager:info("Starting broker: ~p", [BrokerName]),
  compose("up " ++ BrokerName, State).
