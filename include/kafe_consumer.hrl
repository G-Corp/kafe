-record(message, {
          group_id,
          topic,
          partition,
          offset,
          key,
          value}).

-type message() :: #message{}.

