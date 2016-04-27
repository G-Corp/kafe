% @hidden
-module(kafe_consumer_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

%% API.
-export([start_link/2]).

%% gen_server.
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).
-export([fetch/6]).

-record(state, {
          group_id,
          generation_id = -1,
          member_id = <<>>,
          topics = [],
          callback = undefined,
          timer = undefined,
          fetch_interval = ?DEFAULT_CONSUMER_FETCH_INTERVAL,
          fetch_pids = [],
          fetch_size = ?DEFAULT_CONSUMER_FETCH_SIZE
         }).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupID, Options) ->
  gen_server:start_link(?MODULE, [GroupID, Options], []).

%% gen_server.

% @hidden
init([GroupID, Options]) ->
  _ = erlang:process_flag(trap_exit, true),
  FetchInterval = maps:get(fetch_interval, Options, ?DEFAULT_CONSUMER_FETCH_INTERVAL),
  FetchSize = maps:get(fetch_size, Options, ?DEFAULT_CONSUMER_FETCH_SIZE),
  {ok, #state{
          group_id = bucs:to_binary(GroupID),
          callback = maps:get(callback, Options),
          fetch_interval = FetchInterval,
          fetch_size = FetchSize,
          timer = erlang:send_after(FetchInterval, self(), fetch)
         }}.

% @hidden
handle_call(describe, _From, #state{group_id = GroupID} = State) ->
  {reply, kafe:describe_group(GroupID), State};
handle_call(member_id, _From, #state{member_id = MemberID} = State) ->
  {reply, MemberID, State};
handle_call({member_id, MemberID}, _From, State) ->
  {reply, ok, State#state{member_id = MemberID}};
handle_call(generation_id, _From, #state{generation_id = GenerationID} = State) ->
  {reply, GenerationID, State};
handle_call({generation_id, GenerationID}, _From, State) ->
  {reply, ok, State#state{generation_id = GenerationID}};
handle_call(topics, _From, #state{topics = Topics} = State) ->
  {reply, Topics, State};
handle_call({topics, Topics}, _From, State) ->
  {reply, ok, State#state{topics = Topics}};
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(fetch, #state{group_id = GroupID,
                          fetch_interval = FetchInterval,
                          fetch_size = FetchSize,
                          topics = Topics,
                          generation_id = GenerationID,
                          member_id = MemberID,
                          callback = Callback,
                          fetch_pids = []} = State) ->
  Pids = [erlang:spawn_link(?MODULE, fetch, [T, FetchSize, GroupID, GenerationID, MemberID, Callback]) || T <- Topics],
  {noreply, State#state{
              timer = erlang:send_after(FetchInterval, self(), fetch),
              fetch_pids = Pids}};
handle_info(fetch, #state{fetch_interval = FetchInterval} = State) ->
  lager:debug("Previous fetch not terminated!"),
  {noreply, State#state{
              timer = erlang:send_after(FetchInterval, self(), fetch)}};
handle_info({'EXIT', Pid, Reason}, #state{fetch_pids = FetchPids} = State) ->
  lager:debug("Fetch ~p terminated: ~p", [Pid, Reason]),
  {noreply, State#state{fetch_pids = lists:delete(Pid, FetchPids)}};
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  lager:info("Terminate server !!!"),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% @hidden
fetch({Topic, Partitions}, Size, GroupID, GenerationID, MemberID, Callback) ->
  lager:debug("Fetch ~p offsets for ~p:~p, ~p", [Size, Topic, Partitions, GroupID]),
  NoError = kafe_error:code(0),
  case kafe:offset([Topic]) of
    {ok, [#{name := Topic, partitions := PartitionsList}]} ->
      lists:foreach(
        fun(#{id := PartitionID,
              offsets := [Offset|_],
              error_code := NoError1}) ->
            case ((NoError1 == NoError) and
                  lists:member(PartitionID, Partitions) and
                  (Offset - 1 > -1)) of
              true ->
                case kafe:offset_fetch(GroupID, [{Topic, [PartitionID]}]) of
                  {ok, [#{name := Topic,
                          partitions_offset := [#{error_code := NoError,
                                                  offset := OffsetFetch,
                                                  partition := PartitionID}]}]}
                    when OffsetFetch + 1 =< Offset -1 ->
                    Offsets = lists:sublist(lists:seq(OffsetFetch + 1, Offset - 1), Size),
                    Max = lists:max(Offsets),
                    lager:debug("Fetch ~p", [Offsets]),
                    case kafe:offset_commit(GroupID, GenerationID, MemberID, -1,
                                            [{Topic, [{PartitionID, Max, <<>>}]}]) of
                      {ok, _} ->
                        lists:foreach(fun(O) ->
                                          case kafe:fetch(-1, Topic, #{partition => PartitionID, offset => O}) of
                                            {ok, #{topics :=
                                                   [#{name := Topic,
                                                      partitions :=
                                                      [#{error_code := NoError,
                                                         message := #{key := Key,
                                                                      offset := O,
                                                                      value := Value},
                                                         partition := PartitionID}]}]}} ->
                                              _ = erlang:apply(Callback, [Topic, PartitionID, O, Key, Value]);
                                            _ ->
                                              lager:error("Faild to fetch message #~p topic ~p:~p in group ~p", [O, Topic, PartitionID, GroupID])
                                          end
                                      end, Offsets);
                      Error1 ->
                        lager:debug("Faild to commit offet ~p for topic ~p:~p in group ~p : ~p", [Max, Topic, PartitionID, GroupID, Error1])
                    end;
                  {ok, _} ->
                    ok;
                  Error ->
                    lager:debug("Faild to fetch offset for ~p:~p in group ~p : ~p", [Topic, PartitionID, GroupID, Error])
                end;
              false ->
                ok
            end
        end, PartitionsList),
      erlang:exit(done);
    _ ->
      lager:debug("Can't retrieve offsets for topic ~p in ~p", [Topic, GroupID]),
      erlang:exit(offset_error)
  end.

