-module(kafe_consumer_store_tests).

-include_lib("eunit/include/eunit.hrl").

kafe_consumer_store_base_test_() ->
  {setup,
   fun() ->
       ok
   end,
   fun(_) ->
       ok
   end,
   [
    fun() ->
        ?assertEqual(false, kafe_consumer_store:exist(<<"consumer">>)),
        ?assertEqual(ok, kafe_consumer_store:new(<<"consumer">>)),
        ?assertEqual(true, kafe_consumer_store:exist(<<"consumer">>)),
        ?assertEqual(ok, kafe_consumer_store:delete(<<"consumer">>)),
        ?assertEqual(false, kafe_consumer_store:exist(<<"consumer">>))
    end
   ]}.

kafe_consumer_store_autocreate_test_() ->
  {setup,
   fun() ->
       ok
   end,
   fun(_) ->
       ok
   end,
   [
    fun() ->
        ?assertEqual(false, kafe_consumer_store:exist(<<"consumer">>)),
        ?assertEqual(ok, kafe_consumer_store:insert(<<"consumer">>, key, value)),
        ?assertEqual(1, kafe_consumer_store:count(<<"consumer">>)),
        ?assertEqual({ok, value}, kafe_consumer_store:lookup(<<"consumer">>, key)),
        ?assertEqual(ok, kafe_consumer_store:delete(<<"consumer">>, key)),
        ?assertEqual(0, kafe_consumer_store:count(<<"consumer">>)),
        ?assertEqual(ok, kafe_consumer_store:delete(<<"consumer">>)),
        ?assertEqual(false, kafe_consumer_store:exist(<<"consumer">>))
    end
   ]}.

kafe_consumer_store_insert_lookup_test_() ->
  {setup,
   fun() ->
       ok
   end,
   fun(_) ->
       ok
   end,
   [
    fun() ->
        ?assertEqual(false, kafe_consumer_store:exist(<<"consumer">>)),
        ?assertEqual(ok, kafe_consumer_store:new(<<"consumer">>)),
        ?assertEqual(ok, kafe_consumer_store:insert(<<"consumer">>, key, value)),
        ?assertEqual(1, kafe_consumer_store:count(<<"consumer">>)),
        ?assertEqual({ok, value}, kafe_consumer_store:lookup(<<"consumer">>, key)),
        ?assertEqual(value, kafe_consumer_store:value(<<"consumer">>, key)),
        ?assertEqual({error, undefined}, kafe_consumer_store:lookup(<<"consumer">>, invalid_key)),
        ?assertEqual(undefined, kafe_consumer_store:value(<<"consumer">>, invalid_key)),
        ?assertEqual(ok, kafe_consumer_store:delete(<<"consumer">>)),
        ?assertEqual(false, kafe_consumer_store:exist(<<"consumer">>))
    end
   ]}.

