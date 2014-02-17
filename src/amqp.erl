-module(amqp).

-compile(export_all).

-ifndef(ALONE).
-include_lib("../amqp_client/include/amqp_client.hrl").
-else.
-include_lib("deps/amqp_client/include/amqp_client.hrl").
-endif.

connect(Host) ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Connection, Channel}.

basic_subscribe(Channel, Queue, HandlePid) ->
	amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
	amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue}, HandlePid),
	ok.

basic_handle(Channel, Info, State, HandleFunc) ->
	case Info of
		#'basic.consume_ok'{} ->
			ok;
        {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Body}} ->
            case HandleFunc(Body, State) of
            	ok -> amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag});
            	_ -> noproc
            end,
            ok
    end.

disconnect(Connection, Channel) ->
	ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection),
    ok.






