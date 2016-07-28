-module(kafe_rr_tests).

-include_lib("eunit/include/eunit.hrl").

kafe_rr_test_() ->
  {setup,
   fun() ->
       meck:new(kafe),
       meck:expect(kafe, partitions,
                   fun(<<"t">>) -> [0, 1, 2];
                      (<<"t1">>) -> [0, 1, 2];
                      (<<"t2">>) -> [0, 1, 2, 3];
                      (<<"m">>) -> []
                   end),
       kafe_rr:start_link()
   end,
   fun(_) ->
       kafe_rr:stop()
   end,
   [
    fun() ->
        ?assertEqual(0, kafe_rr:next(<<"t">>)),
        ?assertEqual(1, kafe_rr:next(<<"t">>)),
        ?assertEqual(2, kafe_rr:next(<<"t">>)),
        ?assertEqual(0, kafe_rr:next(<<"t">>))
    end,
    fun() ->
        ?assertEqual(0, kafe_rr:next(<<"t1">>)),
        ?assertEqual(0, kafe_rr:next(<<"t2">>)),
        ?assertEqual(1, kafe_rr:next(<<"t1">>)),
        ?assertEqual(1, kafe_rr:next(<<"t2">>)),
        ?assertEqual(2, kafe_rr:next(<<"t1">>)),
        ?assertEqual(2, kafe_rr:next(<<"t2">>)),
        ?assertEqual(0, kafe_rr:next(<<"t1">>)),
        ?assertEqual(3, kafe_rr:next(<<"t2">>)),
        ?assertEqual(1, kafe_rr:next(<<"t1">>)),
        ?assertEqual(0, kafe_rr:next(<<"t2">>))
    end,
    fun() ->
        ?assertEqual(0, kafe_rr:next(<<"m">>)),
        ?assertEqual(0, kafe_rr:next(<<"m">>))
    end
   ]}.

