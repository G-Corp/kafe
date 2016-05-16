% @hidden
% see https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
-module(kafe_consumer_fsm).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_fsm).

-include("../include/kafe.hrl").

%% API.
-export([start_link/2]).

%% gen_fsm.
-export([init/1]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([terminate/3]).
-export([code_change/4]).

% States
-export([
         dead/2,
         awaiting_sync/2,
         stable/2
        ]).

-record(state, {
          group_id_atom,
          group_id,
          client_id = undefined,
          member_id,
          leader_id = undefined,
          generation_id = undefined,
          topics = [],
          session_timeout,
          members = [],
          protocol_group
         }).

-define(MIN_TIMEOUT, 10).
-define(DEAD_TIMEOUT(_), ?MIN_TIMEOUT).
-define(AWAITING_SYNC_TIMEOUT(_), ?MIN_TIMEOUT).
-define(STABLE_TIMEOUT(State),
        begin
          #state{session_timeout = SessionTimeout} = State,
          bucs:to_integer(SessionTimeout - (0.1 * SessionTimeout))
        end).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupId, Options) when is_map(Options) ->
	gen_fsm:start_link(?MODULE, [GroupId, Options], []).

%% gen_fsm.

% @hidden
init([GroupId, Options]) ->
  lager:info("Start consumer ~p", [GroupId]),
  _ = erlang:process_flag(trap_exit, true),
  SessionTimeout = maps:get(session_timeout, Options, ?DEFAULT_JOIN_GROUP_SESSION_TIMEOUT),
  MemberId = maps:get(member_id, Options, ?DEFAULT_JOIN_GROUP_MEMBER_ID),
  Topics = lists:map(fun
                       ({_, _} = T) -> T;
                       (T) -> {T, kafe:partitions(T)}
                     end, maps:get(topics, Options,
                                   lists:map(fun(#{topic := Topic, partitions := Partitions}) ->
                                                 {Topic, Partitions}
                                             end, ?DEFAULT_GROUP_PARTITION_ASSIGNMENT))),
  State = #state{
             group_id_atom = bucs:to_atom(GroupId),
             group_id = bucs:to_binary(GroupId),
             % client_id
             member_id = MemberId,
             % leader_id
             % generation_id
             topics = Topics,
             session_timeout = SessionTimeout
             % members
             % protocol_group
            },
  setelement(1, next_state(dead, State), ok).

% @hidden
dead(timeout, #state{group_id_atom = GroupIdAtom,
                     group_id = GroupId,
                     member_id = MemberId,
                     topics = Topics,
                     session_timeout = SessionTimeout} = State) ->
  ProtocolTopics = lists:map(fun({Topic, _}) -> Topic;
                                (Topic) -> Topic
                             end, Topics),
  case kafe:join_group(GroupId, #{session_timeout => SessionTimeout,
                                  member_id => MemberId,
                                  protocol_type => ?DEFAULT_JOIN_GROUP_PROTOCOL_TYPE,
                                  protocols => [kafe:default_protocol(
                                                  ?DEFAULT_GROUP_PROTOCOL_NAME,
                                                  ?DEFAULT_GROUP_PROTOCOL_VERSION,
                                                  ProtocolTopics,
                                                  ?DEFAULT_GROUP_USER_DATA)]}) of
    {ok, #{error_code := none,
           generation_id := GenerationId,
           leader_id := LeaderId,
           member_id := NewMemberId,
           protocol_group := ProtocolGroup}} ->
      _ = kafe_consumer:member_id(GroupIdAtom, NewMemberId),
      _ = kafe_consumer:generation_id(GroupIdAtom, GenerationId),
      next_state(State#state{generation_id = GenerationId,
                             leader_id = LeaderId,
                             member_id = NewMemberId,
                             protocol_group = ProtocolGroup});
    {ok, #{error_code := unknown_member_id}} ->
      next_state(dead, State#state{member_id = <<>>});
    {ok, #{error_code := _}} ->
      next_state(dead, State);
    {error, Reason} ->
      lager:info("Join group faild: ~p", [Reason]),
      next_state(State)
  end.

% @hidden
awaiting_sync(timeout, #state{group_id = GroupId,
                              generation_id = GenerationId,
                              member_id = MemberId,
                              leader_id = LeaderId,
                              members = Members,
                              topics = Topics} = State) ->
  GroupAssignment = group_assignment(LeaderId, MemberId, Topics, Members),
  case kafe:sync_group(GroupId, GenerationId, MemberId, GroupAssignment) of
    {ok, #{error_code := none}} ->
      next_state(State);
    {error, Reason} ->
      lager:info("Sync group faild: ~p", [Reason]),
      next_state(State)
  end.

% @hidden
stable(timeout, #state{group_id = GroupId,
                       member_id = MemberId,
                       generation_id = GenerationId} = State) ->
  case kafe:heartbeat(GroupId, GenerationId, MemberId) of
    {ok,#{error_code := none}} ->
      next_state(State);
    {ok,#{error_code := Error}} ->
      lager:info("Heartbeat error: ~p", [Error]),
      next_state(dead, State);
    {error, Reason} ->
      lager:info("Heartbeat faild: ~p", [Reason]),
      next_state(State)
  end.

% @hidden
handle_event(_Event, StateName, State) ->
	{next_state, StateName, State}.

% @hidden
handle_sync_event(_Event, _From, StateName, State) ->
	{reply, ignored, StateName, State}.

% @hidden
handle_info(_Info, StateName, State) ->
	{next_state, StateName, State}.

% @hidden
terminate(_Reason, _StateName, _State) ->
	ok.

% @hidden
code_change(_OldVsn, StateName, State, _Extra) ->
	{ok, StateName, State}.

next_state(#state{group_id = GroupId,
                  group_id_atom = GroupIdAtom,
                  member_id = MemberId} = State) ->
  case kafe:describe_group(GroupId) of
    {ok, [#{error_code := none,
            state := GroupState,
            members := Members}]} ->
      case [T || #{member_assignment := #{partition_assignment := T,
                                          version := V},
                   member_id := M} <- Members, M == MemberId, V =/= -1] of
        [] ->
          ok;
        [Topics] ->
          kafe_consumer:topics(GroupIdAtom, [{T, P} || #{partitions := P, topic := T} <- Topics])
      end,
      {NextState, Timeout} = group_state(State, GroupState),
      {next_state, NextState, State#state{members = Members}, Timeout};
    {ok, [#{error_code := Error}]} ->
      lager:info("Can't get group ~p description : ~p", [GroupId, Error]),
      next_state(dead, State);
    {error, _} ->
      next_state(dead, State)
  end.

next_state(NextState, State) ->
  {NextState1, Timeout} = group_state(State, NextState),
  {next_state, NextState1, State, Timeout}.

group_state(State, Next) when is_binary(Next) ->
  group_state(State, state_by_name(Next));
group_state(_State, dead) ->
  {dead, ?DEAD_TIMEOUT(_State)};
group_state(_State, awaiting_sync) ->
  {awaiting_sync, ?AWAITING_SYNC_TIMEOUT(_State)};
group_state(State, stable) ->
  {stable, ?STABLE_TIMEOUT(State)}.

state_by_name(A) when is_atom(A) -> A;
state_by_name(<<"Dead">>) -> dead;
state_by_name(<<"AwaitingSync">>) -> awaiting_sync;
state_by_name(<<"Stable">>) -> stable.

group_assignment(MemberId, MemberId, Topics, Members) ->
  MemberIDs = [M || #{member_id := M} <- Members],
  [#{member_id => Member,
     member_assignment => #{
       version => ?DEFAULT_GROUP_PROTOCOL_VERSION,
       user_data => ?DEFAULT_GROUP_USER_DATA,
       partition_assignment => [#{topic => T,
                                  partitions => P} || {T, P} <- Assignment, length(P) =/= 0]}}
   || {Member, Assignment} <- lists:zip(MemberIDs, assign(Topics, length(MemberIDs), lists:duplicate(length(MemberIDs), [])))];
group_assignment(_, _, _, _) ->
  [].

assign([], _, Acc) ->
  Acc;
assign([{T, P}|Rest], S, Acc) ->
  Repartition = split_list_parts(P, S),
  Repartition1 = if
                   S > length(Repartition) ->
                     Repartition ++ lists:duplicate(S - length(Repartition), []);
                   true ->
                     Repartition
                 end,
  Repartition2 = lists:zip(lists:duplicate(S, T), Repartition1),
  assign(Rest, S, assign_zip(Acc, Repartition2, [])).

split_list_parts(List, N) ->
  split_list_parts(List, erlang:length(List), N, []).

split_list_parts([], _, _, Acc) ->
  lists:reverse(Acc);
split_list_parts(List, Size, N, Acc) ->
  NE = Size div N,
  case lists:split(NE, List) of
    {[], Rest} ->
      split_list_parts(Rest, Size, N - 1, Acc);
    {R, Rest} ->
      split_list_parts(Rest, Size - erlang:length(R), N - 1, [R|Acc])
  end.

assign_zip([], [], Acc) ->
  Acc;
assign_zip([L|RL], [E|RE], Acc) ->
  assign_zip(RL, RE, [[E|L]|Acc]).

