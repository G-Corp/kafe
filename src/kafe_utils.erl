% @hidden
-module(kafe_utils).

-export([
         broker_id/2,
         broker_name/2
        ]).

broker_id(Host, Port) ->
  bucs:to_atom(bucs:to_string(Host) ++ ":" ++ bucs:to_string(Port)).

broker_name(Host, Port) ->
  bucs:to_string(Host) ++ ":" ++ bucs:to_string(Port).

