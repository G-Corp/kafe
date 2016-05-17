% @author Grégoire Lejeune <gregoire.lejeune@botsunit.com>
% @copyright 2014-2015 Finexkap, 2015 G-Corp, 2015-2016 BotsUnit
% @since 2014
% @doc
% A Kafka client for Erlang
%
% To create a consumer, create a function with 6 parameters :
%
% <pre>
% -module(my_consumer).
%
% -export([consume/6]).
%
% consume(CommitID, Topic, Partition, Offset, Key, Value) ->
%   % Do something with Topic/Partition/Offset/Key/Value
%   ok.
% </pre>
%
% The <tt>consume</tt> function must return <tt>ok</tt> if the message was treated, or <tt>{error, term()}</tt> on error.
%
% Then start a new consumer :
%
% <pre>
% ...
% kafe:start(),
% ...
% kafe:start_consumer(my_group, fun my_consumer:consume/6, Options),
% ...
% </pre>
%
% See {@link kafe:start_consumer/3} for the available <tt>Options</tt>.
%
% In the <tt>consume</tt> function, if you didn't start the consumer with <tt>autocommit</tt> set to <tt>true</tt>, you need to commit manually when you
% have finished to treat the message. To do so, use {@link kafe_consumer:commit/1} with the <tt>CommitID</tt> as parameter.
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
-behaviour(supervisor).

% API
-export([
         start/3
         , stop/1
         , commit/1
         , commit/2
         , clear_commits/1
         , describe/1
         , member_id/1
         , generation_id/1
         , topics/1
        ]).

% Private
-export([
         start_link/2
         , init/1
         , member_id/2
         , generation_id/2
         , topics/2
         , encode_group_commit_identifier/4
         , decode_group_commit_identifier/1
        ]).

% @equiv kafe:start_consumer(GroupID, Callback, Options)
start(GroupID, Callback, Options) ->
  kafe:start_consumer(GroupID, Callback, Options).

% @equiv kafe:stop_consumer(GroupID)
stop(GroupID) ->
  kafe:stop_consumer(GroupID).

% @doc
% Return consumer group descrition
% @end
-spec describe(GroupPIDOrID :: atom() | pid() | binary()) -> {ok, kafe:describe_group()} | {error, term()}.
describe(GroupPIDOrID) ->
  kafe_consumer_sup:call_srv(GroupPIDOrID, describe).

% @equiv commit(GroupCommitIdentifier, #{})
-spec commit(GroupCommitIdentifier :: kafe:group_commit_identifier()) -> ok | {error, term()} | delayed.
commit(GroupCommitIdentifier) ->
  commit(GroupCommitIdentifier, #{}).

% @doc
% Commit the offset (in Kafka) for the given <tt>GroupCommitIdentifier</tt> received in the <tt>Callback</tt> specified when starting the
% consumer group (see {@link kafe:start_consumer/3}
%
% If the <tt>GroupCommitIdentifier</tt> is not the lowerest offset to commit in the group :
% <ul>
% <li>If the consumer was created with <tt>allow_unordered_commit</tt>, the commit is delayed</li>
% <li>Otherwise this function return <tt>{error, cant_commit}</tt></li>
% </ul>
%
% Available options:
%
% <ul>
% <li><tt>retry :: integer()</tt> : max retry (default 0).</li>
% <li><tt>delay :: integer()</tt> : Time (in ms) between each retry.</li>
% </ul>
% @end
-spec commit(GroupCommitIdentifier :: kafe:group_commit_identifier(), Options :: map()) -> ok | {error, term()} | delayed.
commit(GroupCommitIdentifier, Options) ->
  case decode_group_commit_identifier(GroupCommitIdentifier) of
    {Pid, Topic, Partition, Offset} ->
      gen_server:call(Pid, {commit, Topic, Partition, Offset, Options});
    _ ->
      {error, invalid_group_commit_identifier}
  end.

% @doc
% Clear all commits for the given group
% @end
-spec clear_commits(GroupPIDOrID :: atom() | pid() | binary()) -> ok.
clear_commits(GroupPIDOrID) ->
  kafe_consumer_sup:call_srv(GroupPIDOrID, clear_commits).

% @hidden
member_id(GroupID, MemberID) ->
  kafe_consumer_sup:call_srv(GroupID, {member_id, MemberID}).

% @doc
% Return the <tt>member_id</tt> of the consumer
% @end
-spec member_id(GroupPIDOrID :: atom() | pid() | binary()) -> binary().
member_id(GroupID) ->
  kafe_consumer_sup:call_srv(GroupID, member_id).

% @hidden
generation_id(GroupID, GenerationID) ->
  kafe_consumer_sup:call_srv(GroupID, {generation_id, GenerationID}).

% @doc
% Return the <tt>generation_id</tt> of the consumer
% @end
-spec generation_id(GroupPIDOrID :: atom() | pid() | binary()) -> integer().
generation_id(GroupID) ->
  kafe_consumer_sup:call_srv(GroupID, generation_id).

% @hidden
topics(GroupID, Topics) ->
  kafe_consumer_sup:call_srv(GroupID, {topics, Topics}).

% @doc
% Return the topics (and partitions) of the consumer
% @end
-spec topics(GroupPIDOrID :: atom() | pid() | binary()) -> [{binary(), [integer()]}].
topics(GroupID) ->
  kafe_consumer_sup:call_srv(GroupID, topics).

% @hidden
start_link(GroupID, Options) ->
  supervisor:start_link({global, bucs:to_atom(GroupID)}, ?MODULE, [GroupID, Options]).

% @hidden
init([GroupID, Options]) ->
  {ok, {
     #{strategy => one_for_one,
       intensity => 1,
       period => 5},
     [
      #{id => kafe_consumer_srv,
        start => {kafe_consumer_srv, start_link, [GroupID, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_srv]},
      #{id => kafe_consumer_fsm,
        start => {kafe_consumer_fsm, start_link, [GroupID, Options]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [kafe_consumer_fsm]}
     ]
    }}.

encode_group_commit_identifier(Pid, Topic, Partition, Offset) ->
  base64:encode(erlang:term_to_binary({Pid, Topic, Partition, Offset}, [compressed])).

decode_group_commit_identifier(GroupCommitIdentifier) ->
  erlang:binary_to_term(base64:decode(GroupCommitIdentifier)).

