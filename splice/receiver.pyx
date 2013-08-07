import zmq

from libc.stdio cimport printf
from zmq.core.socket cimport Socket
from zmq.core.libzmq cimport *

cdef extern from "stdlib.h" nogil:
    int strncmp(char *a, char *b, size_t n)
    void exit(int status)
    void abort()

ctr_data = "DAT"
ctr_ping = "PIN" # Message is a ping heartbeat
ctr_kill = "KIL" # Message is a request to immediately terminate node
ctr_exit = "EXT"

cdef char*c_ctr_ping = "PIN"
cdef char*c_ctr_kill = "KIL"

def receive(in_sock, out_sock):
    """
    Core receiver loop. Runs outside of the GIL and can thus respond to pings and
    other control messages even if the host python code is busy with something else.
    Unhandled control messages are forwarded.

    :param in_sock: Socket for recieving incoming messages.
    :param out_sock: Socket for forwarding unhandled messages.
    """
    errno = _receive(in_sock, out_sock)
    if errno != 0:
        raise zmq.ZMQError(errno=errno, msg=zmq.strerror(errno))

cdef int _receive(Socket in_sock, Socket out_sock):
    cdef int rc
    cdef int64_t more
    cdef size_t opt_size
    cdef zmq_msg_t msg_id
    cdef zmq_msg_t msg_ctr
    cdef zmq_msg_t msg_data
    cdef char*ctr_data
    cdef void*rcv_handle
    cdef void*fwd_handle
    cdef void*repl_handle
    cdef int should_forward

    rcv_handle = in_sock.handle
    fwd_handle = out_sock.handle

    # Core loop must run outside of GIL
    with nogil:
        opt_size = sizeof(int64_t)

        rc = zmq_msg_init(&msg_id)
        if rc < 0: return zmq_errno()

        rc = zmq_msg_init(&msg_ctr)
        if rc < 0: return zmq_errno()

        rc = zmq_msg_init(&msg_data)
        if rc < 0: return zmq_errno()

        while True:
            # printf("OL WAIT\n")
            rc = zmq_recvmsg(rcv_handle, &msg_id, 0)
            if rc < 0: return zmq_errno()

            rc = zmq_recvmsg(rcv_handle, &msg_ctr, 0)
            if rc < 0: return zmq_errno()

            ctr_data = <char*> zmq_msg_data(&msg_ctr)
            # printf("OL RECV %s %d\n", ctr_data, zmq_msg_size(&msg_ctr))

            rc = zmq_getsockopt(rcv_handle, ZMQ_RCVMORE, <void *> &more, &opt_size)
            if rc < 0: return zmq_errno()
            # printf("OL MORE %d\n", more)

            # Check if the first 3 characters are a ping or kill control message
            if strncmp(ctr_data, c_ctr_ping, 3) == 0:
                # printf("OL PING\n")
                rc = zmq_sendmsg(rcv_handle, &msg_id, ZMQ_SNDMORE)
                if rc < 0: return zmq_errno()

                rc = zmq_sendmsg(rcv_handle, &msg_ctr, 0)
                if rc < 0: return zmq_errno()

                should_forward = False
            elif strncmp(ctr_data, c_ctr_kill, 3) == 0:
                exit(-1)
            else:
                # printf("OL DATA\n")
                # Forward unhandled control messages
                rc = zmq_sendmsg(fwd_handle, &msg_id, ZMQ_SNDMORE)
                if rc < 0: return zmq_errno()

                rc = zmq_sendmsg(fwd_handle, &msg_ctr, ZMQ_SNDMORE if more else 0)
                if rc < 0: return zmq_errno()

                should_forward = True

            # Receive any additional frames and optionally forward them.
            # Additional frames for PING messages will be silently discarded.
            while more:
                rc = zmq_recvmsg(rcv_handle, &msg_data, 0)
                if rc < 0: return zmq_errno()

                rc = zmq_getsockopt(rcv_handle, ZMQ_RCVMORE, <void *> &more, &opt_size)
                if rc < 0: return zmq_errno()

                # printf("IL %s MORE %d\n", ctr_data, more)

                if should_forward:
                    # printf("IL FWD\n")
                    rc = zmq_sendmsg(fwd_handle, &msg_data, ZMQ_SNDMORE if more else 0)
                    if rc < 0: return zmq_errno()

    return 0