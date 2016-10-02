% @hidden
-module(kafe_consumer_commiter).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

-include("../include/kafe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          topic,
          partition,
          group_id,
          commit_store_key,
          commits = []
         }).

start_link(Topic, Partition, GroupID) ->
  gen_server:start_link(?MODULE, [Topic, Partition, GroupID], []).

% @hidden
init([Topic, Partition, GroupID]) ->
  erlang:process_flag(trap_exit, true),
  CommitStoreKey = erlang:term_to_binary({Topic, Partition}),
  kafe_consumer_store:insert(GroupID, {commit_pid, CommitStoreKey}, self()),
  {ok, #state{
          topic = Topic,
          partition = Partition,
          group_id = GroupID,
          commit_store_key = CommitStoreKey,
          commits = []
         }}.

% @hidden
handle_call({commit, Topic, Partition, Offset, GroupID, GenerationID, MemberID, Options}, _From, State) ->
  case kafe_consumer_store:lookup(GroupID, allow_unordered_commit) of
    {ok, true} ->
      unordered_commit(Topic, Partition, Offset, GroupID, GenerationID, MemberID, Options, State);
    _ ->
      ordered_commit(Topic, Partition, Offset, GroupID, GenerationID, MemberID, Options, State)
  end;
handle_call({store_for_commit, Offset}, _From, #state{commits = Commits,
                                                      topic = Topic,
                                                      partition = Partition,
                                                      group_id = GroupID} = State) ->
  GenerationID = kafe_consumer_store:value(GroupID, generation_id),
  MemberID = kafe_consumer_store:value(GroupID, member_id),
  CommitsList = lists:append(Commits,
                             [{{Topic, Partition, Offset, GroupID, GenerationID, MemberID}, false}]),
  lager:debug("STORE FOR COMMIT Offset ~p for Topic ~p, partition ~p", [Offset, Topic, Partition]),
  {reply,
   kafe_consumer:encode_group_commit_identifier(self(), Topic, Partition, Offset, GroupID, GenerationID, MemberID),
   State#state{commits = CommitsList}};
handle_call(remove_commits, _From, State) ->
  {reply, ok, State#state{commits = []}};
handle_call({remove_commit, Topic, Partition, Offset, GroupID, GenerationID, MemberID}, _From, #state{commits = Commits} = State) ->
  case Commits of
    [{{Topic, Partition, Offset, GroupID, GenerationID, MemberID}, _}|Rest] ->
      {reply, ok, State#state{commits = Rest}};
    _ ->
      {reply, {error, not_head_commit}, State}
  end;
handle_call(pending_commits, _From, #state{commits = Commits} = State) ->
  {reply,
   [kafe_consumer:encode_group_commit_identifier(self(), T, P, O, Gr, Gn, M)
    || {{T, P, O, Gr, Gn, M}, _} <- Commits],
   State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, #state{group_id = GroupID, commit_store_key = CommitStoreKey}) ->
  kafe_consumer_store:delete(GroupID, {commit_pid, CommitStoreKey}),
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

unordered_commit(Topic, Partition, Offset, GroupID, GenerationID, MemberID, Options, #state{commits = Commits} = State) ->
  Retry = maps:get(retry, Options, ?DEFAULT_CONSUMER_COMMIT_RETRY),
  Delay = maps:get(delay, Options, ?DEFAULT_CONSUMER_COMMIT_DELAY),
  case lists:keyfind({Topic, Partition, Offset, GroupID, GenerationID, MemberID}, 1, Commits) of
    {{Topic, Partition, Offset, GroupID, GenerationID, MemberID} = Commit, _} ->
      case commit(
             lists:keyreplace(Commit, 1, Commits, {Commit, true}),
             ok, Retry, Delay) of
        {ok, Commits1} ->
          case lists:keyfind({Topic, Partition, Offset, GroupID, GenerationID, MemberID}, 1, Commits1) of
            {{Topic, Partition, Offset, GroupID, GenerationID, MemberID}, true} ->
              {reply, delayed, State#state{commits = Commits1}};
            false ->
              {reply, ok, State#state{commits = Commits1}}
          end;
        {Error, Commits1} ->
          {reply, Error, State#state{commits = Commits1}}
      end;
    false ->
      {reply, {error, invalid_commit_ref}, State}
  end.

ordered_commit(Topic, Partition, Offset, GroupID, GenerationID, MemberID, Options, #state{commits = Commits} = State) ->
  Retry = maps:get(retry, Options, ?DEFAULT_CONSUMER_COMMIT_RETRY),
  Delay = maps:get(delay, Options, ?DEFAULT_CONSUMER_COMMIT_DELAY),
  case Commits of
    [{{Topic, Partition, Offset, GroupID, GenerationID, MemberID}, _}|Commits1] ->
      lager:debug("COMMIT Offset ~p for Topic ~p, partition ~p", [Offset, Topic, Partition]),
      case do_commit(GroupID, GenerationID, MemberID,
                     Topic, Partition, Offset,
                     Retry, Delay, {error, invalid_retry}) of
        {ok, [#{name := Topic,
                partitions := [#{error_code := none,
                                 partition := Partition}]}]} ->
          {reply, ok, State#state{commits = Commits1}};
        {ok, [#{name := Topic,
                partitions := [#{error_code := Error,
                                 partition := Partition}]}]} ->
          {reply, {error, Error}, State};
        Error ->
          {reply, Error, State}
      end;
    _ ->
      {reply, {error, missing_previous_commit}, State}
  end.

commit([], Result, _, _) ->
  {Result, []};
commit([{_, false}|_] = Rest, Result, _, _) ->
  {Result, Rest};
commit([{{Topic, Partition, Offset, GroupID, GenerationID, MemberID}, true}|Rest] = All, ok, Retry, Delay) ->
  lager:debug("COMMIT Offset ~p for Topic ~p, partition ~p", [Offset, Topic, Partition]),
  case do_commit(GroupID, GenerationID, MemberID,
                 Topic, Partition, Offset,
                 Retry, Delay, {error, invalid_retry}) of
    {ok, [#{name := Topic,
            partitions := [#{error_code := none,
                             partition := Partition}]}]} ->
      commit(Rest, ok, Retry, Delay);
    {ok, [#{name := Topic,
            partitions := [#{error_code := Error,
                             partition := Partition}]}]} ->
      {{error, Error}, All};
    Error ->
      {Error, All}
  end.

do_commit(_, _, _, _, _, _, Retry, _, Return) when Retry < 0 ->
  Return;
do_commit(GroupID, GenerationID, MemberID, Topic, Partition, Offset, Retry, Delay, _) ->
  case kafe:offset_commit(GroupID, GenerationID, MemberID, -1,
                          [{Topic, [{Partition, Offset, <<>>}]}]) of
    {ok, [#{name := Topic,
            partitions := [#{error_code := none,
                             partition := Partition}]}]} = R ->
      R;
    Other ->
      _ = timer:sleep(Delay),
      do_commit(GroupID, GenerationID, MemberID, Topic, Partition, Offset, Retry - 1, Delay, Other)
  end.

-ifdef(TEST).
commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  meck:new(kafe_consumer_store),
  meck:expect(kafe_consumer_store, value,
              fun
                (_, generation_id) -> 1;
                (_, member_id) -> <<"memberID">>;
                (_, allow_unordered_commit) -> false
              end),
  meck:expect(kafe_consumer_store, lookup,
              fun
                (_, allow_unordered_commit) -> {ok, false}
              end),
  State0 = #state{topic = <<"topic">>, partition = 0, group_id = <<"FakeGroup">>},
  {reply, CommitID1, State1} = handle_call({store_for_commit, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID2),
  {reply, Commits0, State3} = handle_call(pending_commits, from, State2),
  ?assertEqual(
     [kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
      kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>)],
     Commits0),
  {reply, ok, State4} = handle_call({commit, <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>, #{}}, from, State3),
  {reply, Commits1, _} = handle_call(pending_commits, from, State4),
  ?assertEqual(
     [kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>)],
     Commits1),
  meck:unload(kafe_consumer_store),
  meck:unload(kafe).

commit_generation_change_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  meck:new(kafe_consumer_store),
  meck:expect(kafe_consumer_store, value, [{['_', generation_id], meck:seq([1, 2])},
                                           {['_', member_id], <<"memberID">>},
                                           {['_', allow_unordered_commit], false}]),
  meck:expect(kafe_consumer_store, lookup,
              fun
                (_, allow_unordered_commit) -> {ok, false}
              end),
  State0 = #state{group_id = <<"FakeGroup">>, topic = <<"topic">>, partition = 0},
  {reply, CommitID1, State1} = handle_call({store_for_commit, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID1),

  {reply, CommitID2, State3} = handle_call({store_for_commit, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 2, <<"memberID">>),
     CommitID2),

  {reply, Commits0, State4} = handle_call(pending_commits, from, State3),
  ?assertEqual(
     [kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
      kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 2, <<"memberID">>)],
     Commits0),
  {reply, ok, State5} = handle_call({commit, <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>, #{}}, from, State4),
  {reply, Commits1, _} = handle_call(pending_commits, from, State5),
  ?assertEqual(
     [kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 2, <<"memberID">>)],
     Commits1),
  meck:unload(kafe_consumer_store),
  meck:unload(kafe).

delayed_commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  meck:new(kafe_consumer_store),
  meck:expect(kafe_consumer_store, value,
              fun
                (_, generation_id) -> 1;
                (_, member_id) -> <<"memberID">>;
                (_, allow_unordered_commit) -> true
              end),
  meck:expect(kafe_consumer_store, lookup,
              fun
                (_, allow_unordered_commit) -> {ok, true}
              end),
  State0 = #state{group_id = <<"FakeGroup">>, topic = <<"topic">>, partition = 0},
  {reply, CommitID1, State1} = handle_call({store_for_commit, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID2),
  {reply, Commits, State3} = handle_call(pending_commits, from, State2),
  ?assertEqual(
     [CommitID1, CommitID2],
     Commits),
  {reply, delayed, State4} = handle_call({commit, <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>, #{}}, from, State3),
  {reply, ok, State5} = handle_call({commit, <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>, #{}}, from, State4),
  ?assertMatch({reply, [], _},
               handle_call(pending_commits, from, State5)),
  meck:unload(kafe_consumer_store),
  meck:unload(kafe).

remove_commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  meck:expect(kafe_consumer_store, value,
              fun
                (_, generation_id) -> 1;
                (_, member_id) -> <<"memberID">>;
                (_, allow_unordered_commit) -> false
              end),
  meck:expect(kafe_consumer_store, lookup,
              fun
                (_, allow_unordered_commit) -> {ok, false}
              end),
  State0 = #state{group_id = <<"FakeGroup">>, topic = <<"topic">>, partition = 0},
  {reply, CommitID1, State1} = handle_call({store_for_commit, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID2),
  {reply, Commits0, State3} = handle_call(pending_commits, from, State2),
  ?assertEqual(
     [CommitID1, CommitID2],
     Commits0),
  ?assertEqual({reply, {error, not_head_commit}, State3},
               handle_call({remove_commit, <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>}, from, State3)),
  {reply, ok, State4} = handle_call({remove_commit, <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>}, from, State3),
  {reply, Commits1, State4} = handle_call(pending_commits, from, State4),
  ?assertEqual(
     [CommitID2],
     Commits1),
  meck:unload(kafe_consumer_store),
  meck:unload(kafe).

invalid_commit_test() ->
  meck:new(kafe),
  meck:expect(kafe, offset_commit, fun(_, _, _, _, [{T, [{P, _, _}]}]) ->
                                       {ok, [#{name => T,
                                               partitions => [#{error_code => none,
                                                                partition => P}]}]}
                                   end),
  meck:expect(kafe_consumer_store, value,
              fun
                (_, generation_id) -> 1;
                (_, member_id) -> <<"memberID">>;
                (_, allow_unordered_commit) -> false
              end),
  meck:expect(kafe_consumer_store, lookup,
              fun
                (_, allow_unordered_commit) -> {ok, false}
              end),
  State0 = #state{group_id = <<"FakeGroup">>, topic = <<"topic">>, partition = 0},
  {reply, CommitID1, State1} = handle_call({store_for_commit, 0}, from, State0),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 0, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID1),
  {reply, CommitID2, State2} = handle_call({store_for_commit, 1}, from, State1),
  ?assertEqual(
     kafe_consumer:encode_group_commit_identifier(self(), <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>),
     CommitID2),
  {reply, Commits, State3} = handle_call(pending_commits, from, State2),
  ?assertEqual(
     [CommitID1, CommitID2],
     Commits),
  ?assertMatch({reply, {error, missing_previous_commit}, _},
               handle_call({commit, <<"topic">>, 0, 1, <<"FakeGroup">>, 1, <<"memberID">>, #{}}, from, State3)),
  {reply, ok, State4} = handle_call(remove_commits, from, State3),
  ?assertMatch({reply, [], _},
               handle_call(pending_commits, from, State4)),
  meck:unload(kafe_consumer_store),
  meck:unload(kafe).

-endif.

