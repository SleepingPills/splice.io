import sys
import traceback


class spliceException(Exception):
    pass


class ShutdownInProgress(spliceException):
    pass


class ProcessNotFoundError(spliceException):
    pass


class ProtocolError(spliceException):
    pass


class ChannelClosed(spliceException):
    pass


class ActivationError(spliceException):
    pass


class TimeoutError(spliceException):
    pass


class CompositeError(spliceException):
    def __init__(self, values=None, errors=None):
        self.values = values or []
        self.errors = errors or []


class ProcessError(spliceException):
    def __init__(self, inner=None, url=None, handler_name=None, tb=None):
        super(ProcessError, self).__init__()
        self.inner = inner
        self.sp_proc_url = url
        self.sp_handler_name = handler_name
        if tb is not None and (url is not None):
            self.sp_traceback = format_process_traceback(tb, url.resource, url.address)


def enrich_exception(ex, url, handler_name):
    """
    Enriches the provided exception with extra information useful in debugging
    distributed call stacks.
    """
    tb = traceback.format_exception(*sys.exc_info())
    tb_string = format_process_traceback(tb, url.resource, url.address)

    # If this error has already been enriched, then it wasn't thrown
    # in the current process, therefore we should wrap it
    if hasattr(ex, "sp_proc_url"):
        return ProcessError(ex, url, handler_name, tb_string)

    ex.sp_proc_url = url
    ex.sp_handler_name = handler_name
    ex.sp_traceback = tb_string
    return ex


def full_traceback(ex):
    """
    Unwinds the exception stack and produces a traceback spanning the entire
    process call stack.
    """
    tracebacks = []

    def _walk(ex):
        try:
            tracebacks.append(ex.sp_traceback)
        except AttributeError:
            # It looks like this exception is not enriched
            tracebacks.append("\n +--- Exception '%s' has no traceback information\n" % repr(ex))
        if isinstance(ex, ProcessError):
            _walk(ex.inner)

    _walk(ex)

    return "".join(tracebacks)


def root_error(ex):
    """
    Returns the root error if the argument is a stacked exception.
    Otherwise it simply returns the argument.
    """
    if isinstance(ex, ProcessError):
        return root_error(ex.inner)

    return ex


def format_process_traceback(tb, proc_id, local_address):
    result = ["\n +--- Process Exception Dump:\n",
              " | Process Id: %s\n" % str(proc_id),
              " | Node Addr.: %s\n" % local_address,
              " +--- Traceback"]
    for line in tb:
        if line.endswith("\n"):
            line = line[:-1]
        lines = line.split("\n")
        for line in lines:
            result.append("\n | ")
            result.append(line)
    result.append("\n +--- End of Exception Dump\n")
    return "".join(result)