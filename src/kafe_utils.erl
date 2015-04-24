% @hidden
-module(kafe_utils).

-export([
         broker_id/2,
         broker_name/2
        ]).

broker_id(Host, Port) ->
  eutils:to_atom(eutils:to_string(Host) ++ ":" ++ eutils:to_string(Port)).

broker_name(Host, Port) ->
  eutils:to_string(Host) ++ ":" ++ eutils:to_string(Port).

