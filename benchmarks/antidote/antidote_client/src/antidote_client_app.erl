%%%-------------------------------------------------------------------
%% @doc antidote_client public API
%% @end
%%%-------------------------------------------------------------------

-module(antidote_client_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([test/0]).

start(_StartType, _StartArgs) ->
    antidote_client_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

test() ->
	Register = {<<"a">>, antidote_crdt_register_lww, <<"bucket">>},
	Counter = {<<"k">>, antidote_crdt_counter_pn, <<"bucket">>},
  Map = {<<"test_map">>, antidote_crdt_map_rr, <<"bucket">>},
  Set = {<<".bucket">>, antidote_crdt_set_aw, <<"bucket">>},

	{ok, Pid} = antidotec_pb_socket:start_link({127,0,0,1}, 8087),

	{ok, Tx1} =  antidotec_pb:start_transaction(Pid, ignore, []),

	%{ok, Register_Value} = antidotec_pb:read_objects(Pid, [Register], TxId ),
	ok = antidotec_pb:update_objects(Pid, [
    {Map, update, [
      {{<<"test">>, antidote_crdt_counter_pn}, {increment, 1}}
    ]}
    % {Set, add, <<"test_map">>}
  ], Tx1),

	{ok, _} = antidotec_pb:commit_transaction(Pid, Tx1),

	{ok, Tx2} =  antidotec_pb:start_transaction(Pid, ignore, []),
	{ok, RVal} = antidotec_pb:read_objects(Pid, [Register], Tx2 ),
	{ok, CVal} = antidotec_pb:read_objects(Pid, [Counter], Tx2 ),
  {ok, [MVal]} = antidotec_pb:read_values(Pid, [Map], Tx2 ),
  {ok, [SVal]} = antidotec_pb:read_objects(Pid, [Set], Tx2 ),

  io:format("Map value: ~p ~n", [MVal]),
  % io:format("Set value: ~p ~n", antidotec_set:value(SVal)),

	{ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),

	_Disconnected = antidotec_pb_socket:stop(Pid),
	ok.