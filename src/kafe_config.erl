% @hidden
-module(kafe_config).

-export([
         conf/1
         , conf/2
        ]).

conf(Field) when is_atom(Field) ->
  conf([kafe|Field]);
conf([App|Fields]) when is_list(Fields) ->
  get_conf(Fields, application:get_all_env(App)).

conf(Field, Default) ->
  case conf(Field) of
    undefined ->
      Default;
    X ->
      X
  end.

get_conf(_, undefined) ->
  undefined;
get_conf([], Result) ->
  Result;
get_conf([Field|Rest], Data) ->
  get_conf(Rest, elists:keyfind(Field, 1, Data, undefined)).
