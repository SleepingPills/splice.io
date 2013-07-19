import gevent
import gevent.pool
import inspect

from splice.core import Process, ProcessMeta, Supervisor, ProcessBuilder, config_host, get_host, Proxy, Coroutine

__all__ = ["uri", "runtime", "Process", "ProcessMeta", "Supervisor"]


def sync(future, timeout=None):
    """
    Synchronously evaluate the supplied future, returning the result. Useful for
    creating synchronous messages shorthand:

    >>> result = sync(some_process.some_method(some_arg))
    >>> print result
        1

    :param future: A future or asynchronous result object.
    :return: The value encapsulated by the future.
    """
    return future.get(timeout=timeout)


def syncmany(futures):
    """
    Synchronously evalute a collection of futures, returning a list of results.

    >>> results = syncmany([some_process.some_method(some_arg) for some_process in processes])
    >>> print results
        [1, 2, 3, 4, 5]

    :param futures: A collection of futures. Can be any iterable.
    :return: A list of values.
    """
    return [future.get() for future in futures]


class Runtime(object):
    def __init__(self):
        self._greenpool = gevent.pool.Pool()

    def config(self, address=None, port=None, multiprocessing=True):
        """
        Configures the local runtime and host. Must be called before any processes are
        instantiated or connections to remote hosts made, otherwise the default values will be
        used for all parameters.

        :param address: Optional. Specify an address to bind to. Default is the default
                        IP address of the first network interface.
        :param port: Optional. Specify a port to use. Default is the first available random port.
        :param multiprocessing: Optional. Switches between multiprocessing or multithreading
                                for forking. Multithreading can be useful for debugging but
                                provides little to no performance benefits.
        """
        config_host(address, port, multiprocessing)

    def fork(self, count=1, init=None):
        """
        Creates one or more new hosts ready to accomodate processes.
        :param init: Optional. A function that will be run during the initialization of the host(s).
        :return: One or more remote hosts.
        """
        return self.host.fork(count, init)

    @property
    def greenpool(self):
        """
        Get a pool of greenlets that can be used to execute non-blocking IO in parallel.
        """
        return self._greenpool

    @property
    def host(self):
        """
        The local host instance.
        """
        return get_host()

    def sleep(self, seconds=0):
        """
        Yield execution from the currently running process
        :param seconds: Sleep at least the supplied amount of seconds. Due to cooperative
            scheduling, the actual sleep time might be arbitrarily more.
        """
        gevent.sleep(seconds)

    def connect(self, url):
        """
        Connects to the process on the specified url and returns a proxy.
        :param url: Url of the process
        :return: A proxy for the process
        """
        return self.host.connect(url)

    def spawn(self, ptype_or_gen, *args, **kwargs):
        """
        Spawn a process of the specified type.

        :param ptype_or_gen: The process type or generator function
        :param args: Positional arguments to the process constructor
        :param kwargs: Keyword arguments to the process constructor with some
            reserved keywords that are used to control the spawning:
            sp_id - specifies a custom ID for the process
            sp_super - link the process to the supplied supervisor
            sp_dest - spawn the process on a specified destination host
        :return: The process (or a proxy if it was spawned remotely)
        """
        proc_kwargs = {}

        spawn_kwargs = {}

        for k, v in kwargs.iteritems():
            if "sp_" in k:
                spawn_kwargs[k] = v
            else:
                proc_kwargs[k] = v

        # In case the input is a generator function, we wrap the construction and
        # type in a Coroutine process
        if inspect.isgeneratorfunction(ptype_or_gen):
            args = (ptype_or_gen, args, proc_kwargs)
            ptype_or_gen = Coroutine
            proc_kwargs = {}

        proc_builder = ProcessBuilder(ptype_or_gen, args, proc_kwargs)
        return self._spawn_build(proc_builder, **spawn_kwargs)

    def spawn_scatter(self, destinations, proc_type, *args, **kwargs):
        """
        Batch spawn processes of the specified type on a set of remote nodes.

        :param destinations: An iterable of remote host proxies or addresses
        :param proc_type: The process type
        :param args: Positional arguments to the process constructor
        :param kwargs: Keyword arguments to the process constructor
        :return: List of process proxies
        """
        proc_builder = ProcessBuilder(proc_type, args, kwargs)
        promises = [self._spawn_remote(proc_builder, sp_dest=dest) for dest in destinations]
        return syncmany(promises)

    def _spawn_build(self, builder, sp_id=None, sp_super=None, sp_dest=None):
        if sp_dest is None:
            return self.host.spawn_build(builder, sp_id, sp_super)
        else:
            promise = self._spawn_remote(builder, sp_id, sp_super, sp_dest)
            return promise.get()

    def _spawn_remote(self, builder, sp_id=None, sp_super=None, sp_dest=None):
        # If destination is an address string, connect to the remote host
        if isinstance(sp_dest, str):
            sp_dest = self.host.remote(sp_dest)

        return sp_dest.spawn_build(builder, sp_id, sp_super)

    def stop(self, proc):
        """
        Stop the supplied process (whether local or remote)
        :param proc: Process instance or proxy
        """
        self.host.unschedule(proc.sp_url)

    def start(self):
        """
        Enter an event loop ready to yield execution to incoming requests.
        This function blocks until the local host is explicitly shut down.
        """
        self.host.start()

    def shutdown(self, reason="N/A", remote_node=None):
        """
        Shuts down the local or the optional remote node.
        :param reason: Shutdown reason. Used for logging and information purposes only.
        :param remote_node: If specified, the remote node will be shut down. Can be a proxy
                            of any process running on the remote node, or a string address.
        :return:
        """
        if remote_node is not None:
            if isinstance(remote_node, str):
                self.host.router.send_shutdown(remote_node, reason)
            elif isinstance(remote_node, Proxy):
                self.host.router.send_shutdown(remote_node.sp_url.address, reason)
            else:
                raise ValueError("Remote node must be a string address or Proxy instance")
        else:
            self.host.shutdown("local-node", reason)

    def __reduce__(self):
        # The runtime object should never get serialized, instead the local runtime should be returned on unpickling
        return _get_runtime, ()


runtime = Runtime()


def _get_runtime():
    return runtime

_get_runtime.__safe_for_unpickling__ = True