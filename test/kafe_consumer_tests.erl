-module(kafe_consumer_tests).
-include_lib("eunit/include/eunit.hrl").

-export([can_fetch/0]).

kafe_consumer_topics_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, topics, [{<<"topic1">>, [0, 1, 2]},
                                                          {<<"topic2">>, [0, 1]}])
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertEqual([{<<"topic1">>, 0},
                      {<<"topic1">>, 1},
                      {<<"topic1">>, 2},
                      {<<"topic2">>, 0},
                      {<<"topic2">>, 1}],
                     kafe_consumer:topics(<<"test_cg">>)),
        ?assertEqual([], kafe_consumer:topics(<<"invalid_cg">>))
    end
   ]}.

kafe_consumer_generation_id_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, generation_id, 1)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertEqual(1, kafe_consumer:generation_id(<<"test_cg">>))
    end
   ]}.

kafe_consumer_member_id_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, member_id, <<"member_id">>)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertEqual(<<"member_id">>, kafe_consumer:member_id(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_fetch_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assert(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_fetch_undefined_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch_fun, undefined)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assert(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_fetch_fun_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch_fun, fun() -> true end)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assert(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

can_fetch() -> true.
kafe_consumer_can_fetch_mod_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch_fun, {?MODULE, can_fetch})
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assert(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_not_fetch_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, false),
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch_fun, fun() -> true end)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertNot(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_not_fetch_fun_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch_fun, fun() -> false end)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertNot(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_not_fetch_undef_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:new(<<"test_cg">>)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertNot(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

kafe_consumer_can_not_fetch_exception_test_() ->
  {setup,
   fun() ->
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch, true),
       kafe_consumer_store:insert(<<"test_cg">>, can_fetch_fun, fun() -> throw(test_error) end)
   end,
   fun(_) ->
       kafe_consumer_store:delete(<<"test_cg">>)
   end,
   [
    fun() ->
        ?assertNot(kafe_consumer:can_fetch(<<"test_cg">>))
    end
   ]}.

