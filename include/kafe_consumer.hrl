-record(message, {
          commit_ref,
          topic,
          partition,
          offset,
          key,
          value}).

-type message() :: #message{}.

