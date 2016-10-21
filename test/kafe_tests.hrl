-define(REQ_STATE(CID), #{api_version => 1,
                          correlation_id => CID,
                          client_id => <<"test">>}).

-define(REQ_STATE2(CID, V), #{api_version => V,
                              correlation_id => CID,
                              client_id => <<"test">>}).

-define(D(Call), ?debugFmt("~n---> ~p~n", [Call])).
