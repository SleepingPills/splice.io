import sys
import logging
import threading

import gevent.threadpool
import gevent.event as ge
import gevent.queue as gq
import multiprocessing as mp

from splice import uri, net
from splice.exception import ShutdownInProgress, ProcessNotFoundError, enrich_exception, ActivationError
from splice.util import log, get_ip
from splice.green import Tasklet

_address = None
_port = 0
_host_proc = None
_proc_ctr = mp.Process


def _host_run_fork(init, sender):
    if init is not None:
        init()

    log.setup_defaults()

    # On linux, forking would result in a host object
    # already being available (including all the sockets in the router).
    # By forcefully creating a new one, we'll actually be able to communicate
    # with the target process.
    host = get_host(force_new=True)

    sender.send(host.address)
    sender.close()

    host.start()


def config_host(address=None, port=None, multiprocessing=True):
    global _address
    global _port
    global _proc_ctr

    if _host_proc is not None:
        raise ValueError("Cannot configure local host once it is already running.")

    if address is not None:
        _address = address
    if port is not None:
        _port = port

    _proc_ctr = mp.Process if multiprocessing else threading.Thread


def get_host(force_new=False):
    global _host_proc
    if _host_proc is None or force_new:
        global _address
        global _port
        if _address is None:
            _address = get_ip()

        _host_proc = Host(_address, _port)

    return _host_proc


class ProcessBuilder(object):
    def __init__(self, proc_type, args, kwargs):
        self.proc_type = proc_type
        self.args = args
        self.kwargs = kwargs

    def build(self, pid):
        proc = self.proc_type(*self.args, **self.kwargs)
        proc.sp_init(pid, self)
        return proc


class Host(object):
    def __init__(self, host, port, timeout=120):
        self._processes = {}
        self._child_procs = []
        self._child_procs_lock = threading.Lock()
        self._pool = gevent.threadpool.ThreadPool(mp.cpu_count() * 10)
        self._timeout = timeout

        self.router = net.Router(self.shutdown, self._on_request, host, port, timeout)

        self._logger = logging.getLogger(self.router.address + "/host")

        self._shutting_down = False

        # Create a process for the local host
        host_builder = ProcessBuilder(HostProcess, [self], {})
        self.local = self.spawn_build(host_builder, sp_id="host")

    def _on_request(self, remote_node_address, request_id, recipient_id, handler_name, args, kwargs):
        try:
            try:
                process = self._processes[recipient_id]
            except KeyError:
                raise ProcessNotFoundError("Process %s does not exist" % recipient_id)

            result = process._sp_sync_exec(handler_name, args, kwargs)

            self.router.send_response(remote_node_address, request_id, result, None)
        except (Exception, StopIteration, GeneratorExit) as ex:
            self.router.send_response(remote_node_address, request_id, None, ex)

    @property
    def address(self):
        return self.router.address

    def fork(self, count=1, init=None):
        """
        Creates one or more new hosts ready to accomodate processes.
        :param init: Optional. A function that will be run during the initialization of the host(s).
        :return: One or more remote host addresses.
        """
        with self._child_procs_lock:
            # If we are in the midst of shutdown then don't attempt to fork
            if self._shutting_down:
                raise ShutdownInProgress()

            def _create_subprocess(_):
                r, w = mp.Pipe(duplex=False)
                p = _proc_ctr(target=_host_run_fork, args=(init, w))
                p.daemon = True
                p.start()

                # Wait a finite period of time for the address to come
                success = r.poll(self._timeout)
                # If we didn't get a response, return None as address
                return (r.recv(), p) if success else (None, p)

            cproc_results = self._pool.map(_create_subprocess, xrange(count))

            # Stash away the child processes that responded in time. Terminate those which didn't.
            remote_host_addresses = []
            for remote_address, proc in cproc_results:
                if remote_address is not None:
                    self._child_procs.append(proc)
                    remote_host_addresses.append(remote_address)
                else:
                    self._logger.error("Forked process %s failed to respond, terminating.", proc.pid)
                    proc.terminate()

            return remote_host_addresses

    def remote(self, address):
        """
        Connects to the host process on the remote node and returns a proxy

        :param address: address of remote node
        :return: Remote runtime
        """
        return self.connect(uri.url("host", address))

    def spawn_build(self, builder, sp_id=None, sp_super=None):
        """
        Builds and hosts a process
        :param builder: The builder that is capable of constructing this process
        :param sp_id: An optional id/name of the process
        :param sp_super: An optional supervisor of the process
        :return:
        """
        process = builder.build(sp_id or uri.uid())
        if sp_super is not None:
            process.sp_link(sp_super)
        self.schedule(process)
        return process

    def schedule(self, proc):
        """
        Schedules the supplied process locally. Used for hosting an existing process (e.g. after relocation).
        :param proc:
        :return:
        """
        self._processes[proc.sp_id] = proc
        proc.sp_activate(self.router.address)

    def unschedule(self, proc_url):
        """
        Deschedules the supplied process. Used to stop processing for the process.
        :param proc_url:
        :return:
        """
        # If the address is local, deschedule the process
        if proc_url.address == self.router.address:
            proc = self._processes.pop(proc_url.resource)
            proc.sp_passivate()
        else:
            # Otherwise deschedule it on the remote host
            remote_node = self.remote(proc_url.address)
            remote_node.unschedule(proc_url)

    def connect(self, url):
        """
        Connects to the process on the specified url and returns a proxy.
        :param url:
        :return:
        """
        address = url.address
        proc_id = url.resource

        # If the address is local then just return the process
        if address == self.router.address:
            return self._processes[proc_id]

        return Proxy(url, self.router)

    def start(self):
        self._logger.info("Starting server event loop.")
        self.router.start_serve()
        self._logger.info("Exiting server event loop.")

    def shutdown(self, requester_address, reason):
        self._shutting_down = True
        self._logger.warn("Shutting down on request from node %s with reason '%s'", requester_address, reason)
        # Clean up first
        self._cleanup()
        sys.exit(0)

    def _cleanup(self):
        with self._child_procs_lock:
            for proc in self._child_procs:
                proc.terminate()

            self._child_procs[:] = []

    def __del__(self):
        self._cleanup()


def _rehydrate_proxy(url):
    return get_host().connect(url)


_rehydrate_proxy.__safe_for_unpickling__ = True


class Proxy(object):
    def __init__(self, url, router):
        self.sp_id = url.resource
        self.sp_url = url
        self._sp_hash = hash(self.sp_id)
        self._sp_router = router

    def __iter__(self):
        return SyncProxyIterator(self)

    def next(self):
        return self._sp_route("next", (), {})

    def __str__(self):
        return "Proxy<%s>" % str(self.sp_url)

    def __getattr__(self, name):
        def route(*args, **kwargs):
            return self._sp_route(name, args, kwargs)

        return route

    def _sp_route(self, name, args, kwargs):
        url = self.sp_url
        return self._sp_router.send_twoway_request(url.address, url.resource, name, args, kwargs)

    def __reduce__(self):
        return _rehydrate_proxy, (self.sp_url,)

    def __hash__(self):
        return self._sp_hash

    def __eq__(self, other):
        if not isinstance(other, Proxy):
            return False
        return self._sp_hash == other._sp_hash


class SyncProxyIterator(object):
    def __init__(self, proxy):
        self.__proxy = proxy

    def next(self):
        return self.__proxy.next().get()


class ProcessMeta(type):
    def __new__(cls, cls_name, cls_bases=None, cls_dict=None, **kwargs):
        def augment(name, attr):
            # We don't augment non-functions, non-public attributes, class or static methods
            if ((name[0] == "_")
                or (name.startswith("sp_"))
                or (not hasattr(attr, "__call__"))
                or isinstance(attr, classmethod)
                or isinstance(attr, staticmethod)):
                return name, attr

            return name, lambda proc, *args, **kwargs: proc._sp_schedule_request(name, attr, args, kwargs)

        # Augment candidate methods on the class
        aug_dct = dict([augment(name, value) for name, value in cls_dict.items()])

        if not any(map(lambda bc: issubclass(bc, Process), cls_bases)):
            # If the class does not Inherit from Process, add it here. In some cases, e.g. when
            # a proccess inherits from a Supervisor base class, we don't need to add Process explicitly.
            cls_bases = (Process,) + cls_bases

        return super(ProcessMeta, cls).__new__(cls, cls_name, cls_bases, aug_dct)


class Process(object):
    def sp_init(self, pid, proc_builder):
        self.sp_id = pid
        self.__compensator = None
        self.__relocating = False
        self.__mailbox = gq.Queue()
        self.__mailbox_processor = None

        self.__proc_builder = proc_builder

    def sp_activate(self, address):
        if self.__mailbox_processor is not None:
            raise ActivationError("Tried to activate process %s twice" % self.sp_id)

        self.sp_url = uri.url(self.sp_id, address)

        def _loop(mbox, executor):
            try:
                while True:
                    msg = mbox.get()
                    executor(*msg)
            finally:
                # Execute the exit callback
                self.sp_exit()

        # Spawn and schedule mailbox processor loop
        self.__mailbox_processor = Tasklet.spawn(_loop, self.__mailbox, self._sp_execute_request)

    def sp_passivate(self):
        self.__mailbox_processor.kill(block=False)
        self.__mailbox_processor = None

    def sp_link(self, supervisor):
        self.__compensator = supervisor
        supervisor.supervise(self, type(self), self.__proc_builder)

    def sp_unlink(self):
        self.__compensator.unsupervise(self)
        self.__compensator = None

    def sp_exit(self):
        pass

    def _sp_sync_exec(self, handler_name, args, kwargs):
        handler = getattr(self, handler_name)
        return handler(*args, **kwargs).get()

    def _sp_schedule_request(self, handler_name, orig_handler, args, kwargs):
        future = ge.AsyncResult()
        self.__mailbox.put_nowait((future, handler_name, orig_handler, args, kwargs))
        return future

    def _sp_execute_request(self, future, handler_name, orig_handler, args, kwargs):
        try:
            future.set(orig_handler(self, *args, **kwargs))
        except BaseException as ex:
            ex = enrich_exception(ex, self.sp_url, handler_name)

            if self.__compensator is not None:
                self.__compensator.compensate(self, ex)
            else:
                future.set_exception(ex)

    def __str__(self):
        return "Process<%s>" % self.sp_id

    def __reduce__(self):
        if self.__relocating:
            # If marked for relocation, serialize the process itself
            raise NotImplementedError("Process relocation not supported yet")
        else:
            # Otherwise just send a proxy
            return _rehydrate_proxy, (self.sp_url,)


class Coroutine(Process):
    __metaclass__ = ProcessMeta

    def __init__(self, generator_thunk, gen_args, gen_kwargs):
        super(Coroutine, self).__init__()
        self.__generator = generator_thunk(*gen_args, **gen_kwargs)

    def send(self, value):
        return self.__generator.send(value)

    def next(self):
        return self.__generator.send(None)

    def close(self):
        return self.__generator.close()

    def throw(self, etype, *args):
        return self.__generator.throw(etype, *args)

    def sp_exit(self):
        super(Coroutine, self).sp_exit()
        self.__generator.close()

    def __iter__(self):
        return SyncCoroutineIterator(self)


class SyncCoroutineIterator(object):
    def __init__(self, coro):
        self.__coro = coro

    def next(self):
        return self.__coro.next().get()


class Supervisor(Process):
    def sp_init(self, pid, proc_builder):
        super(Supervisor, self).sp_init(pid, proc_builder)
        self.__subjects = {}

    def supervise(self, proc, proc_builder):
        self.__subjects[proc.sp_id] = (proc, proc_builder)

    def unsupervise(self, proc):
        del self.__subjects[proc.sp_id]

    def compensate(self, failed_proc, exception):
        pass


class HostProcess(object):
    __metaclass__ = ProcessMeta

    def __init__(self, runtime):
        self.__runtime = runtime

    def spawn_build(self, builder, sp_id=None, sp_super=None):
        return self.__runtime.spawn_build(builder, sp_id, sp_super)

    def unschedule(self, proc_url):
        self.__runtime(proc_url)

    def ping(self, message):
        return "Got: %s" % message