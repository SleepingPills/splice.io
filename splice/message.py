import marshal
import splice.serialization as ps
from zmq.core.message import Frame


class NoCopy(object):
    """
    A class for providing non-copying transmission of messages.
    """
    def __init__(self, frames):
        self.frames = frames

    def get_items(self):
        return [frame.bytes for frame in self.frames]

    @staticmethod
    def from_items(cls, items):
        return NoCopy([Frame(item) for item in items])


def can_ctr(ctr_type, contents=None):
    header = [ctr_type]
    return header if contents is None else header + contents


def can_msg(msg_type, contents=None):
    return [Frame(msg_type)] if contents is None else [Frame(msg_type)] + contents


def uncan_msg(multipart):
    # remote_node_address, ctr_type, msg_type, contents
    return multipart[0].bytes, multipart[1].bytes, multipart[2].bytes, multipart[3:]


def can_request(request_id, recipient_id, handler_name, args, kwargs):
    # If the one and only argument is a NoCopy object, then send it correctly
    if len(args) == 1 and (isinstance(args[0], NoCopy)):
        header = Frame(marshal.dumps((request_id, recipient_id, handler_name, True)))
        return [header] + args[0].frames

    header = Frame(marshal.dumps((request_id, recipient_id, handler_name, False)))
    return [header, Frame(ps.dumps_robust((args, kwargs)))]


def uncan_request(multipart):
    request_id, recipient_id, handler_name, nocopy = marshal.loads(multipart[0].bytes)

    if nocopy:
        return request_id, recipient_id, handler_name, NoCopy(frames=multipart[1:]), {}

    args, kwargs = ps.loads(multipart[1].bytes)
    return request_id, recipient_id, handler_name, args, kwargs


def can_response(request_id, ex, result):
    if isinstance(result, NoCopy):
        return [Frame(marshal.dumps((request_id, True)))] + result.frames

    return [Frame(marshal.dumps((request_id, False))), ps.dumps_robust((ex, result))]


def uncan_response(multipart):
    request_id, nocopy = marshal.loads(multipart[0].bytes)

    if nocopy:
        return request_id, None, NoCopy(frames=multipart[1:])

    ex, result = ps.loads(multipart[1].bytes)
    return request_id, ex, result


def can_disconnect_channels(chan_addresses, reason):
    return [Frame(marshal.dumps((chan_addresses, reason)))]


def uncan_disconnect_channels(multipart):
    return marshal.loads(multipart[0].bytes)