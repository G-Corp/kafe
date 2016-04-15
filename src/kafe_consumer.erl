% @author Grégoire Lejeune <gregoire.lejeune@botsunit.com>
% @copyright 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit
% @since 2014
% @doc
% A Kafka client for Erlang
%
% To create a consumer, use this behaviour :
%
% <pre>
% -module(my_consumer).
% -behaviour(kafe_consumer).
%
% -export([init/1, consume/3]).
%
% init(Args) ->
%   {ok, Args}.
%
% consume(Offset, Key, Value) ->
%   % Do something with Offset/Key/Value
%   ok.
% </pre>
%
% Then start a new consumer :
%
% <pre>
% ...
% kafe:start(),
% ...
% kafe:start_consumer(my_group, my_consumer, Options),
% ...
% </pre>
%
% When you are done with your consumer, stop it :
%
% <pre>
% ...
% kafe:stop_consumer(my_group),
% ...
% </pre>
% @end
-module(kafe_consumer).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_fsm).

-callback init(Args :: list()) -> {ok, any()} | ignore.
-callback consume(Offset :: integer(),
                  Key :: binary(),
                  Value :: binary()) -> ok.

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

-define(DEAD_TIMEOUT(_), 10).
-define(AWAITING_SYNC_TIMEOUT(_), 10).
-define(STABLE_TIMEOUT(State),
        begin
          #state{session_timeout = SessionTimeout} = State,
          bucs:to_integer(SessionTimeout - (0.1 * SessionTimeout))
        end).

%% API.

% @hidden
-spec start_link(atom(), map()) -> {ok, pid()}.
start_link(GroupId, Options) ->
	gen_fsm:start_link({global, GroupId}, ?MODULE, [GroupId, Options], []).

%% gen_fsm.

% @hidden
init([GroupId, Options]) ->
  lager:info("Start consumer ~p", [GroupId]),
  _ = erlang:process_flag(trap_exit, true),
  SessionTimeout = maps:get(session_timeout, Options, ?DEFAULT_JOIN_GROUP_SESSION_TIMEOUT),
  MemberId = maps:get(member_id, Options, ?DEFAULT_JOIN_GROUP_MEMBER_ID),
  Topics = maps:get(topics, Options, lists:map(fun(#{topic := Topic, partitions := Partitions}) ->
                                                   {Topic, Partitions}
                                               end, ?DEFAULT_GROUP_PARTITION_ASSIGNMENT)),
  State = #state{
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
  setelement(1, next_state(State), ok).

% @hidden
dead(timeout, #state{group_id = GroupId,
                     member_id = MemberId,
                     topics = Topics,
                     session_timeout = SessionTimeout} = State) ->
  lager:info("Group dead : join!"),
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
      next_state(State#state{generation_id = GenerationId,
                             leader_id = LeaderId,
                             member_id = NewMemberId,
                             protocol_group = ProtocolGroup});
    {ok, #{error_code := unknown_member_id}} ->
      next_state(dead, State#state{member_id = <<>>});
    {error, Reason} ->
      lager:info("Join group faild: ~p", [Reason]),
      next_state(State)
  end.

% @hidden
awaiting_sync(timeout, #state{group_id = GroupId,
                              generation_id = GenerationId,
                              member_id = MemberId,
                              members = Members,
                              topics = Topics} = State) ->
  lager:info("AwaitingSync : Sync!"),
  case kafe:sync_group(GroupId, GenerationId, MemberId, group_assignment(MemberId, Topics, Members)) of
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
  lager:info("Stable: heartbeat"),
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
terminate(_Reason, _StateName, #state{group_id = GroupId} = _State) ->
  lager:info("Terminate consumer ~s", [GroupId]),
	ok.

% @hidden
code_change(_OldVsn, StateName, State, _Extra) ->
	{ok, StateName, State}.

next_state(#state{group_id = GroupId} = State) ->
  case kafe:describe_group(GroupId) of
    {ok, [#{error_code := none,
            state := GroupState,
            members := Members}]} ->
      {NextState, Timeout} = group_state(State, GroupState),
      lager:info("=> ~p in ~pms", [NextState, Timeout]),
      {next_state, NextState, State#state{members = Members}, Timeout};
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

group_assignment(MemberId, Topics, Members) ->
  {HasMemberId,
   GroupAssignment} = lists:foldl(
                        fun(#{member_id := MemberId1,
                              member_assignment := MemberAssignment}, {HasMemberId, MembersAcc}) ->
                            {if
                               MemberId1 == MemberId -> true;
                               true -> HasMemberId
                             end, [#{member_id => MemberId1,
                                     member_assignment => MemberAssignment}|MembersAcc]}
                        end, {false, []}, Members),
  if
    HasMemberId -> GroupAssignment;
    true ->
      [#{member_id => MemberId,
         member_assignment => #{
           version => ?DEFAULT_GROUP_PROTOCOL_VERSION,
           user_data => ?DEFAULT_GROUP_USER_DATA,
           partition_assignment => lists:map(fun
                                               ({T, P}) ->
                                                 #{topic => T,
                                                   partitions => P};
                                               (T) ->
                                                 #{topic => T,
                                                   partitions => maps:keys(maps:get(T, kafe:topics(), #{}))}
                                             end, Topics)
          }}|GroupAssignment]
  end.

