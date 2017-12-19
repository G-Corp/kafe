-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([{parse_transform, lager_transform}]).

-define(RETRY(Expr), ?RETRY(Expr, 10000)).

-define(RETRY(Expr, Timeout),
        fun Try(RemainingTime) ->
          try
            Expr
          catch
            _:_ when RemainingTime > 0 ->
              lager:debug("Evaluation of '~s' failed, waiting before retrying...", [??Expr]),
              timer:sleep(1000),
              Try(RemainingTime - 1000)
          end
        end(Timeout)).
