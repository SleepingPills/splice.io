import os
import traceback
import threading
import zmq
import zmq.core.poll as zpoll
import zmq.green as gzmq
import logging
import time

from splice import uri, message
from splice.exception import ChannelClosed
from splice.green import Tasklet
from splice.reciever import ctr_ping, ctr_data, ctr_exit, receive
from gevent.event import AsyncResult

# Data message types
msg_handshake = "HSK"  # Handshake to establish a two way connection between nodes
msg_request = "REQ"  # Remote function call request to a process
msg_response = "REP"  # Response message as a counterpart to a previous request
_msg_disconnect_channels = "CDT"  # Directs the execution loop to kill all pending request for the supplied remote nodes


def prepend_protocol(address):
    return "tcp://" + address


def full_address_string(host, port):
    return prepend_protocol(host + ":" + str(port))


def extract_frame_data(frames):
    return [frame.bytes for frame in frames]


def _create_socket(ctx, sock_type):
    sock = ctx.socket(sock_type)
    # Set linger to 120s, so that there is a grace period for outgoing messages to be sent, but
    # the process won't deadlock indefinitely when terminating a context.
    sock.linger = 120
    return sock


class Channel(object):
    def __init__(self, router_address, ctx, remote_node_address):
        self._remote_node_address = remote_node_address
        self._push_socket = _create_socket(ctx, gzmq.PUSH)
        self._push_socket.identity = router_address
        self._push_socket.connect(remote_node_address)
        self._requests = {}

    def send_twoway_request(self, request_id, recipient_id, handler_name, args, kwargs):
        future = AsyncResult()
        self._requests[request_id] = future

        msg_req = message.can_request(request_id, recipient_id, handler_name, args, kwargs)
        self.send(message.can_msg(msg_request, msg_req))

        return future

    def send(self, contents, ctr_type=ctr_data):
        multipart_msg = message.can_ctr(ctr_type, contents)
        self._push_socket.send_multipart(multipart_msg, copy=False)

    def complete_request(self, request_id, result, ex):
        future = self._requests.pop(request_id)

        if ex is not None:
            future.set_exception(ex)
        else:
            future.set(result)

    def close(self, reason):
        for future in self._requests.values():
            future.set_exception(ChannelClosed(self._remote_node_address, reason))

        self._push_socket.close()


class Router(object):
    def __init__(self, on_shutdown, on_request, host, port, timeout):
        self._on_shutdown = on_shutdown
        self._on_request = on_request
        self._timeout = timeout

        self._req_identity = 0
        self._connections = {}
        self._green_ctx = gzmq.Context()
        self._ctx = zmq.Context()

        recv_server_sock = _create_socket(self._ctx, zmq.XREP)

        # If port is not specified, then generate a random uid as our id. Otherwise
        # we ensure that the id stays constant for the same host:port pair
        if not port:
            port = recv_server_sock.bind_to_random_port(prepend_protocol(host))
        else:
            recv_server_sock.bind(full_address_string(host, port))

        self.address = full_address_string(host, port)
        self._logger = logging.getLogger(self.address + "/router")

        self._logger.debug("Constructing internal exec loop sockets")
        exec_loop_address = "inproc://" + uri.uid()
        exec_loop_sock = _create_socket(self._green_ctx, gzmq.PULL)
        exec_loop_sock.bind(exec_loop_address)

        recv_exec_sink = _create_socket(self._green_ctx, gzmq.PUSH)
        recv_exec_sink.connect(exec_loop_address)
        hb_exec_sink = _create_socket(self._green_ctx, gzmq.PUSH)
        hb_exec_sink.connect(exec_loop_address)

        self._logger.debug("Scheduling execution loop tasklet")
        self._exec_loop_task = Tasklet.spawn(self._exec_loop, exec_loop_sock)
        # Spin up the execution loop tasklet but just long enough for it to start listening
        # on the exec loop reciever
        self._exec_loop_task.join(1e-3)

        self._recieve_loop_thread = threading.Thread(target=self._recieve_loop, args=(recv_server_sock, recv_exec_sink))
        self._recieve_loop_thread.daemon = True
        self._recieve_loop_thread.start()

        self._heartbeat_loop_thread = threading.Thread(target=self._heartbeat_loop, args=(hb_exec_sink,))
        self._heartbeat_loop_thread.daemon = True
        self._heartbeat_loop_thread.start()

    def start_serve(self):
        self._exec_loop_task.join()

    def send_twoway_request(self, remote_node_address, recipient_id, handler_name, args, kwargs):
        req_id = self._get_next_req_id()

        try:
            conn = self._connections[remote_node_address]
        except KeyError:
            conn = self._connect(remote_node_address)

        return conn.send_twoway_request(req_id, recipient_id, handler_name, args, kwargs)

    def send_response(self, remote_node_address, request_id, result, ex):
        try:
            channel = self._connections[remote_node_address]
            msg_resp = message.can_response(request_id, result, ex)
            channel.send(message.can_msg(msg_response, msg_resp))
        except KeyError:
            self._logger.error("Can't send response %s to %s, channel doesn not exist",
                               request_id, remote_node_address)

    def send_shutdown(self, remote_node_address, reason):
        self._send_shutdown(remote_node_address, reason, ctr_exit)

    def _send_shutdown(self, remote_node_address, reason, level):
        # Don't send handshake for the shutdown request
        channel = self._connect(remote_node_address, send_handshake=False)
        del self._connections[remote_node_address]
        channel.send([reason], ctr_type=level)
        channel.close(reason)

    def disconnect(self, remote_node_address, reason):
        try:
            channel = self._connections.pop(remote_node_address)
            msg_disconnect = message.can_disconnect_channels([remote_node_address], reason)
            msg_disconnect = message.can_msg(_msg_disconnect_channels, msg_disconnect)
            channel.send(msg_disconnect)
            channel.close(reason)
        except KeyError:
            self._logger.warn("Attempted to disconnect from a node with no connection.")

    def _connect(self, remote_node_address, send_handshake=True):
        try:
            return self._connections[remote_node_address]
        except KeyError:
            self._logger.debug("Connecting to %s", remote_node_address)
            self._connections[remote_node_address] = channel = Channel(self.address,
                                                                       self._green_ctx,
                                                                       remote_node_address)

            if send_handshake:
                # Send a handshake to the remote node to make sure it has a connection back to us.
                self._logger.debug("Sending handshake request to %s", remote_node_address)
                channel.send(message.can_msg(msg_handshake))

            return channel

    def _get_next_req_id(self):
        req_id = self._req_identity
        self._req_identity += 1
        return req_id

    def _loop_wrapper(self, func, log):
        while True:
            try:
                func()
            except zmq.ZMQError as err:
                if err.errno == zmq.ETERM:
                    log.warn("Context terminated")
                else:
                    log.error("Fatal ZMQ error:\n%s", traceback.format_exc())
                break
            except (Exception, StopIteration, GeneratorExit):
                log.error("Unhandled exception:\n%s", traceback.format_exc())

    def _exec_loop(self, exec_loop_sock):
        log = logging.getLogger(self.address + "/router-exec-loop")

        def handle_handshake(remote_node_address, _):
            self._logger.debug("Recieved handshake from %s", remote_node_address)
            self._connect(remote_node_address, send_handshake=False)

        def handle_request(remote_node_address, payload):
            # Needs to be in a tasklet, since if the underlying code does non-blocking IO
            # we want the exec loop to start executing the next request immediately.
            Tasklet.spawn(self._on_request, remote_node_address, *message.uncan_request(payload))

        def handle_response(remote_node_address, payload):
            request_id, result, ex = message.uncan_response(payload)
            try:
                conn = self._connections[remote_node_address]
                conn.complete_request(request_id, result, ex)
            except KeyError:
                log.error("Can't deliver response %s from %s. No matching request or channel does not exist",
                          request_id, remote_node_address)

        def handle_disconnect_channel(_, payload):
            disconnect_node_addresses, reason = message.uncan_disconnect_channels(payload)
            for address in disconnect_node_addresses:
                try:
                    log.warn("Closing channel to %s with reason '%s'. Outstanding requests will be killed",
                             address, reason)
                    channel = self._connections.pop(address)
                    channel.close(reason)
                except KeyError:
                    log.warn("Attempted to close channel to %s but it doesn't exist", address)

        msg_handlers = {
            msg_handshake: handle_handshake,
            msg_request: handle_request,
            msg_response: handle_response,
            _msg_disconnect_channels: handle_disconnect_channel
        }

        log.debug("Starting execution loop")

        def _loop():
            msg_canned = exec_loop_sock.recv_multipart(copy=False)

            # ctr_type is discarded at the moment
            remote_node_address, ctr_type, msg_type, payload_multipart = message.uncan_msg(msg_canned)

            if ctr_type == ctr_data:
                handler = msg_handlers[msg_type]
                handler(remote_node_address, payload_multipart)
            elif ctr_type == ctr_exit:
                self._on_shutdown(remote_node_address, msg_type)
            else:
                log.warn("Recieved message with unkown control type '%s'", ctr_type)

        self._loop_wrapper(_loop, log)

        log.debug("Exiting execution loop")

    def _recieve_loop(self, server_sock, exec_loop_sock):
        log = logging.getLogger(self.address + "/router-recv-loop")

        log.debug("Starting recieve loop")

        try:
            receive(server_sock, exec_loop_sock)
        except:
            # In case the core reciever loop failed, we exit
            log.error("Fatal error in receive loop:\n%s", traceback.format_exc())
            os._exit(-1)

        log.debug("Exiting recieve loop")

    def _heartbeat_loop(self, exec_loop_sock):
        log = logging.getLogger(self.address + "/router-heartbeat-loop")

        hb_socks_registry = {}
        endpoints = {}

        log.debug("Starting heartbeat loop")

        def _loop():
            # Make a copy of the connections registry. Copy is atomic due to the GIL.
            remote_node_addresses = set(self._connections.keys())

            # Add any connections that we are not monitoring
            for remote_node_address in remote_node_addresses:
                if remote_node_address not in hb_socks_registry:
                    hb_socket = _create_socket(self._ctx, zmq.XREQ)
                    # The identity of the heartbeat socket should be constant for any given
                    # local-node and remote-node
                    hb_socket.identity = self.address + "->" + remote_node_address
                    # Stash away the remote endpoint for each socket (this is somehow not exposed
                    # by the socket itself)
                    endpoints[hb_socket] = remote_node_address
                    hb_socket.connect(remote_node_address)
                    hb_socks_registry[remote_node_address] = hb_socket

            # Remove heartbeat sockets that we no longer need
            for remote_node_address in hb_socks_registry.keys():
                if remote_node_address not in remote_node_addresses:
                    log.debug("Removing disconnected node %s from heartbeat registry.", remote_node_address)
                    sock = hb_socks_registry.pop(remote_node_address)
                    del endpoints[sock]
                    sock.close()

            remote_node_count = len(hb_socks_registry)
            log.debug("Pinging %d remote nodes in registry", remote_node_count)

            # If there are no sockets to watch, skip the pinging bit
            if not remote_node_count:
                # Wait a bit before retrying
                time.sleep(self._timeout)
                return

            # Get a list of the actual sockets
            hb_socks = hb_socks_registry.values()

            # Send a ping to all sockets
            for sock in hb_socks:
                sock.send_multipart(message.can_ctr(ctr_ping))

            # Wait at most <timeout> seconds for a reply.
            time.sleep(self._timeout)

            # Get all sockets that responded (wait a bit extra if there are no responses at all)
            resp, _, _ = zpoll.select(hb_socks, [], [], timeout=10)

            # Get a list of nodes which did not respond in time
            failures = set(hb_socks).difference(resp)

            for sock in resp:
                # TODO: Add some handling for the replies. At the moment we just throw away them away.
                sock.recv_multipart(zmq.NOBLOCK)

            num_failures = len(failures)
            if num_failures > 0:
                log.warn("%d nodes failed to respond", num_failures)

                dead_node_addresses = [endpoints[failed_hb_sock] for failed_hb_sock in failures]

                # Send a message to the main execution loop that it should kill all pending
                # requests on the affected channels
                msg_disconnect = message.can_disconnect_channels(dead_node_addresses, "TIMEOUT")
                msg_disconnect = message.can_msg(_msg_disconnect_channels, msg_disconnect)
                msg_disconnect = message.can_ctr(ctr_data, msg_disconnect)
                exec_loop_sock.send_multipart([self.address] + msg_disconnect, copy=False)

        self._loop_wrapper(_loop, log)

        log.debug("Exiting heartbeat loop")