% @hidden
-module(kafe_cst).

-export([
         new/0
         , attach_srv/1
         , attach_sup/1
         , detach/0
         , lookup_srvpid/1
         , lookup_suppid/1
        ]).

new() ->
  ets:new(kafe_cgsrv2srvpid, [public, named_table]),
  ets:new(kafe_srvpid2cgsrv, [public, named_table]),
  ets:new(kafe_cgsup2suppid, [public, named_table]),
  ets:new(kafe_suppid2cgsup, [public, named_table]),
  ok.

attach_srv(Name) ->
  BName = bucs:to_binary(Name),
  ets:insert(kafe_cgsrv2srvpid, {BName, self()}),
  ets:insert(kafe_srvpid2cgsrv, {self(), BName}).

attach_sup(Name) ->
  BName = bucs:to_binary(Name),
  ets:insert(kafe_cgsup2suppid, {BName, self()}),
  ets:insert(kafe_suppid2cgsup, {self(), BName}).

detach() ->
  case ets:lookup(kafe_srvpid2cgsrv, self()) of
    [{SrvPID, Name}] ->
      case ets:lookup(kafe_cgsup2suppid, Name) of
        [{Name, SupPID}] ->
          ets:delete(kafe_suppid2cgsup, SupPID),
          ets:delete(kafe_cgsup2suppid, Name);
        [] ->
          ok
      end,
      ets:delete(kafe_srvpid2cgsrv, SrvPID),
      ets:delete(kafe_cgsrv2srvpid, Name);
    [] ->
      ok
  end.

lookup_srvpid(Name) ->
  BName = bucs:to_binary(Name),
  case ets:lookup(kafe_cgsrv2srvpid, BName) of
    [] ->
      {error, invalid};
    [{BName, PID}] ->
      {ok, PID}
  end.

lookup_suppid(Name) ->
  BName = bucs:to_binary(Name),
  case ets:lookup(kafe_cgsup2suppid, BName) of
    [] ->
      {error, invalid};
    [{BName, PID}] ->
      {ok, PID}
  end.

