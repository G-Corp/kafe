-module(poolgirl).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API.
-export([
         start_link/0,
         add_pool/4,
         remove_pool/1,
         checkout/1,
         checkin/1
        ]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
          pools = #{},
          workers = #{},
          assigned = #{},
          assigned_ref = #{}
         }).

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% @doc
% poolgirl:add_pool(test, 5, 20, {kafe_conn, start_link, [{127,0,0,1}, 9092]}).
% @end
-spec add_pool(atom(), integer(), integer(), {atom(), atom(), list()}) -> {ok, integer()} | {error, term()}.
add_pool(Name, Size, ChunkSize, MFArgs) ->
  gen_server:call(?MODULE, {add_pool, Name, Size, ChunkSize, MFArgs}).

-spec remove_pool(atom()) -> ok | {error, term()}.
remove_pool(Name) ->
  gen_server:call(?MODULE, {remove_pool, Name}).

% @doc
% W = poolgirl:checkout(test).
% @end
-spec checkout(atom()) -> {ok, pid()} | {error, term()}.
checkout(Pool) ->
  gen_server:call(?MODULE, {checkout, Pool}).

% @doc
% poolgirl:checkin(W).
% @end
-spec checkin(pid()) -> ok | {error, term()}.
checkin(Worker) ->
  gen_server:call(?MODULE, {checkin, Worker}).

%% gen_server.

% @hidden
init([]) ->
	{ok, #state{}}.

% @hidden
handle_call({add_pool, Name, Size, ChunkSize, MFArgs}, _From, State) ->
  case poolgirl_sup:add_pool(Name, MFArgs) of
    {ok, SupervisorPid} ->
      {State1, PoolSize} = add_workers(Name,
                                       init_pool_wokkers(SupervisorPid,
                                                         Name,
                                                         Size,
                                                         ChunkSize,
                                                         MFArgs,
                                                         State)),
      {reply, {ok, PoolSize}, State1};
    Error -> {reply, Error, State}
  end;
handle_call({remove_pool, _Name}, _From, State) ->
	{reply, ignored, State}; % TODO
handle_call({checkout, Name}, _From, #state{pools = Pools, assigned = Assigned, assigned_ref = AssignedRef} = State) ->
  case maps:get(Name, Pools, undefined) of
    #{workers := Workers} ->
      PidsAssigned = maps:get(Name, Assigned, []),
      case first_available_worker(Workers, PidsAssigned) of
        false ->
          {reply, {error, no_available_worker}, add_workers(Name, State)};
        Pid ->
          {State1, _} = add_workers(Name, State#state{assigned = maps:put(Name, [Pid|PidsAssigned], Assigned),
                                                      assigned_ref = maps:put(Pid, Name, AssignedRef)}),
          {reply, {ok, Pid}, State1}
      end;
    undefined ->
      {reply, {error, unknow_pool}, State}
  end;
handle_call({checkin, Worker}, _From, #state{assigned = Assigned,
                                              assigned_ref = AssignedRef} = State) ->
  case maps:get(Worker, AssignedRef, undefined) of
    undefined ->
      {reply, {error, unknow_worker}, State};
    Name ->
      AssignedPids = maps:get(Name, Assigned, []),
      case lists:member(Worker, AssignedPids) of
        false ->
          {reply, {error, unassigned_worker}, State};
        true ->
          {reply, ok, State#state{assigned = maps:put(Name, lists:delete(Worker, AssignedPids), Assigned),
                                  assigned_ref = maps:remove(Worker, AssignedRef)}}
      end
  end;
handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

% @hidden
handle_cast(_Msg, State) ->
	{noreply, State}.

% @hidden
handle_info({'DOWN', MRef, _, _, _}, #state{pools = Pools, workers = Workers} = State) ->
  try
    PoolName = maps:get(MRef, Workers),
    #{workers := Pids} = Pool = maps:get(PoolName, Pools),
    Pids1 = lists:keydelete(MRef, 2, Pids),
    {noreply, State#state{workers = maps:remove(MRef, Workers),
                     pools = maps:put(PoolName, Pool#{workers => Pids1,
                                                      size => length(Pids1)}, Pools)}}
  catch
    _:_ ->
    {noreply, State}
  end;
handle_info(_Info, State) ->
	{noreply, State}.

% @hidden
terminate(_Reason, _State) ->
	ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

init_pool_wokkers(SupervisorPid, Name, Size, ChunkSize,
                  {Module, Function, Args},
                  #state{pools = Pools} = State) ->
  Pool = #{name => Name,
           supervisor => SupervisorPid,
           workers => [],
           size => 0,
           initial_size => Size,
           chunk_size => ChunkSize,
           module => Module,
           function => Function,
           args => Args},
  State#state{pools = maps:put(Name, Pool, Pools)}.

add_workers(Name, #state{pools = Pools, workers = Workers, assigned = Assigned} = State) ->
  #{supervisor := SupervisorPid,
    args := Args,
    size := Size,
    initial_size := InitialSize,
    chunk_size := ChunkSize,
    workers := Pids} = Pool = maps:get(Name, Pools),
  PidsAssigned = maps:get(Name, Assigned, []),
  AddSize = if
              Size == 0 -> InitialSize;
              length(PidsAssigned) == length(Pids) -> ChunkSize;
              true -> 0
            end,
  if
    AddSize == 0 ->
      {State, length(Pids)};
    true ->
      {Pids1, Workers2} = lists:foldl(
                            fun(_, {Acc, Workers1}) ->
                                case supervisor:start_child(SupervisorPid, Args) of
                                  {ok, Pid} ->
                                    MRef = erlang:monitor(process, Pid),
                                    {[{Pid, MRef}|Acc],
                                     maps:put(MRef, Name, Workers1)};
                                  {ok, Pid, _} ->
                                    MRef = erlang:monitor(process, Pid),
                                    {[{Pid, MRef}|Acc],
                                     maps:put(MRef, Name, Workers1)};
                                  {error, _} ->
                                    {Acc, Workers1}
                                end
                            end, {[], Workers}, lists:seq(1, AddSize)),
      N = length(Pids) + length(Pids1),
      {State#state{pools = maps:put(Name,
                                    Pool#{workers => Pids1 ++ Pids,
                                          size => N},
                                    Pools),
                   workers = Workers2}, N}
  end.

first_available_worker([], _) ->
  false;
first_available_worker([{Pid, _}|Rest], Assigned) ->
  case lists:member(Pid, Assigned) of
    true -> first_available_worker(Rest, Assigned);
    false -> Pid
  end.

