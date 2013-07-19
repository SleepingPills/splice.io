import sys
import logging
import itertools
import splice.util.log as spl

from splice import ProcessMeta, runtime
from datetime import datetime, timedelta
from bintrees import FastRBTree
from collections import deque

from splice.util import retry
from splice.cluster.infra import Session, RemoteCloud
from splice.exception import full_traceback
from splice.green import Tasklet
from splice.uri import url


class NodePool(object):
    def __init__(self, nodes, session_name, logger):
        nodeq = deque()
        for node in nodes:
            for _ in xrange(node.cpu_count):
                nodeq.appendleft(node)

        self._nodeq = nodeq
        self._session_name = session_name
        self._logger = logger

    def spawn_workers(self, count, init=None):
        # Do not attempt to spawn more workers than available capacity
        count = min(count, len(self._nodeq))

        buckets = {}
        for _ in xrange(count):
            node = self._nodeq.pop()
            bucket_size = buckets.setdefault(node.proc, 0)
            buckets[node.proc] = bucket_size + 1

        results = []
        for nproc, bucket_size in buckets.iteritems():
            self._logger.debug("Session %s: spawning %d workers on node %s",
                               self._session_name, bucket_size, nproc)
            result = nproc.spawn_workers(bucket_size, init=init)
            results.append((nproc, result))

        return results

    def __len__(self):
        return len(self._nodeq)


class Master(object):
    __metaclass__ = ProcessMeta

    def __init__(self, node_timeout):
        self._logger = logging.getLogger(self.__class__.__name__)

        self._nodes = {}
        self._sessions = {}
        self._sessions_by_owner = {}
        self._keepalive_queue = FastRBTree()
        self._priority_queue = FastRBTree()
        self._node_timeout = node_timeout
        self._culling_timer = runtime.greenpool.spawn(self._cull_dead_nodes)

    def get_session(self, name, owner=None, dep_server=None, work_dir=None, worker_count=None, init=None):
        try:
            session = self._sessions[name]
            session.dep_cache.set_dependency_server(dep_server)
            return session
        except KeyError:
            if owner is None:
                raise ValueError("An owner must be provided for new sessions")

            if work_dir is None:
                raise ValueError("Valid working directory required to create a new session")

            if dep_server is None:
                raise ValueError("Dependency server must be provided to create a new session")

            session = Session(name, owner, dep_server, worker_count, self._spawn_workers, work_dir, init)

            self._sessions[name] = session
            self._sessions_by_owner.setdefault(owner, {})[name] = session

            return RemoteCloud(name, owner, session.hub, session.created_on, len(session.workers), self)

    def _spawn_workers(self, name, owner, worker_count, init):
        all_nodes = itertools.imap(lambda nd: nd.itervalues(), self._priority_queue.values())
        all_nodes = itertools.chain.from_iterable(all_nodes)
        node_pool = NodePool(all_nodes, name, self._logger)
        node_pool_size = len(node_pool)
        if worker_count is None:
            worker_count = node_pool_size
        self._logger.info("Creating session %s:%s with %d workers", owner, name, worker_count)
        # We can only ever have as many workers as there are processors in the cluster
        if worker_count > node_pool_size:
            self._logger.warning("Session %s: requested worker count %d will be capped to %d",
                                 name, worker_count, node_pool_size)

            worker_count = node_pool_size

        workers = []

        while len(workers) < worker_count and (len(node_pool) > 0):
            results = node_pool.spawn_workers(worker_count - len(workers), init=init)
            for nproc, result in results:
                try:
                    worker_batch = result.get()
                    workers.extend(worker_batch)
                except Exception as ex:
                    self._logger.error("Session %s: failed to spawn workers on node %s due to error:\n%s",
                                       name, nproc, full_traceback(ex))

        return workers

    def shutdown_session(self, name):
        session = self._sessions.pop(name)
        owner_sessions = self._sessions_by_owner[session.owner]
        del owner_sessions[name]

        # Carry out the shutdown operation in the background
        Tasklet.spawn(session.shutdown)

    def node_update(self, node_proc, cpu_count, cpu_usage, ram_total, ram_usage):
        # Remove the node from the queues if it is already registered
        if node_proc in self._nodes:
            node = self._nodes[node_proc]
            self._dequeue(node)
        else:
            # Create a new node info if it doesn't exist yet
            node = NodeInfo(node_proc, cpu_count)
            self._nodes[node_proc] = node

        # Update load based on a simple formula of tenancy and resource usage
        node.update(cpu_usage + ram_usage, cpu_usage, ram_total, ram_usage)

        self._logger.debug("Recieved ping %s", node)

        # Enqueue the node again
        self._enqueue(node)

    def node_info(self):
        return self._nodes.values()

    def shutdown(self):
        """
        Initiate cluster wide shutdown
        """
        self._logger.warn("Shutting down cluster")
        self._culling_timer.kill()

        for node in self._nodes.values():
            self._logger.info("Shutting down node %s", node.proc)
            retry(lambda: node.proc.shutdown(), logger=self._logger)

    def _cull_dead_nodes(self):
        """
        Remove the node so that it cannot be included in new sessions.
        """
        while True:
            dead_nodes = list(self._keepalive_queue[:datetime.now() - timedelta(seconds=self._node_timeout)].values())
            dead_node_count = len(dead_nodes)
            if dead_node_count > 0:
                self._logger.info("Culling %d nodes that are no longer responding.", dead_node_count)
                for node_dicts in dead_nodes:
                    for node in node_dicts.values():
                        self._logger.info("Deleting dead node %s", node.proc)
                        self._delete_node(node)
            else:
                self._logger.info("No dead nodes.")

            runtime.sleep(self._node_timeout)

    def _delete_node(self, node):
        del self._nodes[node.proc]

        self._dequeue(node)

    def _enqueue(self, node):
        self._add_to_queue(self._keepalive_queue, node, node.last_ping)
        self._add_to_queue(self._priority_queue, node, node.load)

    def _dequeue(self, node):
        self._delete_from_queue(self._keepalive_queue, node.proc, node.last_ping)
        self._delete_from_queue(self._priority_queue, node.proc, node.load)

    @staticmethod
    def _add_to_queue(queue, node, key):
        queue.setdefault(key, {})[node.proc] = node

    @staticmethod
    def _delete_from_queue(queue, node_id, key):
        kq_nodes = queue.get(key)
        del kq_nodes[node_id]
        if not len(kq_nodes):
            queue.discard(key)


class NodeInfo(object):
    def __init__(self, proc, cpu_count):
        self.proc = proc
        self.cpu_count = cpu_count
        self.load = self.cpu_usage = self.ram_total = self.ram_used = self.ram_free = 0
        self.last_ping = datetime.now()

    def update(self, load, cpu_usage, ram_total, ram_percent):
        self.load = load
        self.cpu_usage = cpu_usage
        self.ram_total = ram_total
        self.ram_used = ram_total * (1 - ram_percent)
        self.ram_free = ram_total * ram_percent
        self.last_ping = datetime.now()

    def __str__(self):
        return "NodeInfo(%s, cpu_count=%d, cpu_usage=%f%%, ram_total=%.2fGB, ram_used=%.2fGB, ram_free=%.2fGB)" \
               % (self.proc.sp_id, self.cpu_count, self.cpu_usage, self.ram_total, self.ram_used, self.ram_free)

    def __repr__(self):
        return self.__str__()


def connect(address):
    master_url = url("splice-master", address)
    return runtime.connect(master_url)


def run_master(node_timeout=60, port=None, log_level=None):
    spl.setup_defaults(log_level)

    _logger = logging.getLogger(__name__)

    _logger.info("Starting cluster head on port %s with node timeout of %ds", port, node_timeout)
    runtime.config("127.0.0.1", port)
    runtime.spawn(Master, node_timeout, sp_id="splice-master")
    _logger.info("Cluster head node online")
    runtime.start()
    _logger.warn("Exiting master process")
    sys.exit()