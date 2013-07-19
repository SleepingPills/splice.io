import os
import re
import sys
import logging
import tempfile
import traceback
import itertools
import __builtin__

from collections import deque
from datetime import datetime
from splice import runtime, ProcessMeta, sync
from splice.serialization.dependency import DependencyError, EggPackager, get_packager
from splice.datastruct import chop
from splice.datastruct.trie import Trie
from splice.util import try_delete_dir
from splice.exception import CompositeError

_default_ignore = ["(distutils.*)*(enthought.*)*(IPython.*)*(splice.*)*(termios.*)*(builtins.*)*(_xmlplus.*)*"]


class ImportHook(object):
    def __init__(self, worker, ignored_deps):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._imported_deps = Trie()
        self._worker = worker
        self._ignored_deps = [re.compile(pattern) for pattern in ignored_deps]
        self._builtin_import = __builtin__.__import__
        __builtin__.__import__ = self._import

    def _import(self, name, globals={}, locals={}, fromlist=[], level=-1):
        try:
            return self._attempt_local_import(name, globals, locals, fromlist, level)
        except ImportError:
            imported, idx = self._imported_deps.match_prefix(name)
            if imported:
                self._logger.info("%s already loaded, skipping client check for %s.", name, name[:idx])
                # If a parent or this module has been imported before, do not call client
                raise

            for cr in self._ignored_deps:
                if any(cr.match(name).groups()):
                    # If this module is on the ignore list, do not call client
                    self._logger.warn("%s is on ignore list, skipping client check.", name)
                    raise

            # Try to pull dependency from client and import again
            try:
                self._logger.warn("%s could not be found locally, attempting to download from client", name)
                self._worker._pull_dependency(name)
                self._logger.debug("%s pulled down from client", name)
            except DependencyError:
                raise ImportError("%s could not be located either locally or on the client", name)
            return self._attempt_local_import(name, globals, locals, fromlist, level)

    def _attempt_local_import(self, name, globals={}, locals={}, fromlist=[], level=-1):
        module = self._builtin_import(name, globals, locals, fromlist, level)
        self._imported_deps.add(name)
        return module


class Worker(object):
    __metaclass__ = ProcessMeta

    def __init__(self, dep_cache):
        self._logger = logging.getLogger(self.__class__.__name__)

        self._dep_cache = dep_cache
        self._import_hook = ImportHook(self, _default_ignore)

    def apply(self, func, args, kwargs):
        return func(*args, **kwargs)

    def map(self, coordinator, func, args, kwargs):
        batch = sync(coordinator.pull())
        while batch is not None:
            idx, items = batch
            results = []
            errors = []

            for item in items:
                try:
                    results.append(func(item, *args, **kwargs))
                except Exception as ex:
                    errors.append((item, ex, traceback.format_exc()))

            coordinator.push(idx, results, errors)

            # Get the next batch
            batch = sync(coordinator.pull())

        return

    def _pull_dependency(self, module_name):
        eggfile = sync(self._dep_cache.pull(module_name))

        # Egg files need to be individually appended to sys.path
        if eggfile:
            self._logger.debug("Registering %s", eggfile)
            sys.path.append(eggfile)


class ScatterGather(object):
    """
    Implements a naive work queue/gatherer algorithm, but more
    intelligent distribution mechanisms that split work based on computational
    intensity can be added as well.
    """
    __metaclass__ = ProcessMeta

    # The factor by which we should divide the working set beyond the worker count
    _subdivision = 4

    def __init__(self, sequence, worker_count):
        self._logger = logging.getLogger(self.__class__.__name__)
        # Chop the sequence up into equal batches.
        batches = chop(sequence, worker_count * self._subdivision)
        self._batches = deque([(i, b) for i, b in enumerate(batches)])
        self._results = [[]] * len(batches)
        self._errors = []

    def pull(self):
        try:
            return self._batches.pop()
        except IndexError:
            return None

    def push(self, idx, results, errors):
        self._results[idx] = results
        self._errors.extend(errors)

    def get_results(self):
        # Flatten out the results, maintaining the original order
        flat_results = list(itertools.chain.from_iterable(self._results))
        return flat_results, self._errors


class Hub(object):
    __metaclass__ = ProcessMeta

    def __init__(self, workers):
        self._workers = workers
        self._logger = logging.getLogger(self.__class__.__name__)

    def apply(self, func, args, kwargs):
        futures = [worker.apply(func, args, kwargs) for worker in self._workers]
        return [future.get() for future in futures]

    def map(self, func, sequence, args, kwargs):
        coordinator = runtime.spawn(ScatterGather, sequence, len(self._workers))
        self._logger.debug("Created coordinator")
        waithandles = [worker.map(coordinator, func, args, kwargs) for worker in self._workers]
        self._logger.info("Waiting for tasks to finish")

        # Wait until all workers finish
        for handle in waithandles:
            sync(handle)

        self._logger.info("Tasks finished - collecting results")

        # Return the results from the coordinator
        return sync(coordinator.get_results())


class DependencyCache(object):
    __metaclass__ = ProcessMeta

    def __init__(self, dep_server, dep_dir):
        self._logger = logging.getLogger(self.__class__.__name__)

        self._dep_server = dep_server
        self._dep_dir = dep_dir
        self._cache = {}

    def set_dependency_server(self, dep_server):
        self._dep_server = dep_server

    def pull(self, module_name):
        self._logger.info("Got request for dependency %s", module_name)

        def _install(module):
            pkg = sync(self._dep_server.pull(module))
            if not pkg:
                return None

            pkg.deploy(self._dep_dir)
            self._logger.info("Successfully installed package %s", pkg.name)
            return pkg

        try:
            promise = self._cache[module_name]
        except KeyError:
            # If we didn't encounter this dependency before, start the download and flag the installation req
            promise = self._cache[module_name] = runtime.greenpool.spawn(_install, module_name)

        packager = promise.get()

        if packager is None:
            # If we didn't get anyhing from upstream, raise a dependency error
            self._logger.info("Failed to pull dependency %s", module_name)
            raise DependencyError("Dependency not found on client")

        return os.path.join(self._dep_dir, packager.filename) if isinstance(packager, EggPackager) else None


class DependencyServer(object):
    __metaclass__ = ProcessMeta

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def pull(self, module_name):
        self._logger.info("Got request for dependency %s", module_name)
        try:
            module = __import__(module_name, fromlist=[""])
            packager = get_packager(module)
            packager.package()
            return packager
        except ImportError:
            self._logger.warn("Dependency %s not found", module_name)
            return None


class StubDependencyServer(object):
    def pull(self, _):
        return None


class WorkerInit(object):
    def __init__(self, work_dir, scratch_dir, dep_dir, callback=None):
        self.work_dir = work_dir
        self.scratch_dir = scratch_dir
        self.dep_dir = dep_dir
        self.callback = callback

    def __call__(self):
        # Setuptools has a few race conditions when two or more processes load eggs at exactly the same time
        # The following fix makes sure that each process uses a unique extraction place
        try:
            path = tempfile.mkdtemp(prefix="worker-", dir=self.scratch_dir)

            # If using setuptools set the temp egg cache to be in the temp dir
            import pkg_resources

            pkg_resources.set_extraction_path(path)
        except (ImportError, ValueError):
            # Ignore any failures patching setuptools. There is not much more we can do.
            pass

        # Add the session directory to sys.path so that downloaded dependencies are visible
        sys.path.append(self.dep_dir)
        # Add the current actual working dir to sys path in case we are running a private cluster
        sys.path.append(os.getcwd())

        # Change to the proper working dir
        os.chdir(self.work_dir)

        if self.callback is not None:
            self.callback()


class Session(object):
    def __init__(self, name, owner, dep_server, worker_count, worker_builder, work_dir, init):
        self.name = name
        self.owner = owner
        self.created_on = datetime.utcnow()

        self._logger = logging.getLogger("Session(%s,%s)" % (name, owner))

        # Create dependency directory
        dep_dir = os.path.join(work_dir, "%s-%s-deps" % (owner, name))
        if os.path.exists(dep_dir):
            try:
                try_delete_dir(dep_dir, self._logger)
            except:
                self._dir_purge_failed(dep_dir)

        os.mkdir(dep_dir)

        # Create scratch dir
        scratch_dir = os.path.join(work_dir, "%s-%s-scratch" % (owner, name))
        if os.path.exists(scratch_dir):
            try:
                try_delete_dir(scratch_dir, self._logger)
            except:
                self._dir_purge_failed(scratch_dir)

        os.mkdir(scratch_dir)

        whost_init = WorkerInit(work_dir, scratch_dir, dep_dir, init)

        # Spawn 1 additional extra worker for the hub and dependency cache
        hosts = worker_builder(name, owner, worker_count + 1, whost_init)
        hhost = hosts[0]
        whosts = hosts[1:]
        try:
            self.dep_cache = runtime.spawn(DependencyCache, dep_server, dep_dir, sp_dest=hhost)
            self.workers = runtime.spawn_scatter(whosts, Worker, self.dep_cache)
            self.hub = runtime.spawn(Hub, self.workers, sp_dest=hhost)
        except:
            self._logger.error("Error during session initialization:\n%s", traceback.format_exc())
            for host in hosts:
                runtime.shutdown(reason="Cleanup", remote_node=host)
            raise

        self.dep_dir = dep_dir
        self.scratch_dir = scratch_dir
        self._logger.info("Created session %s:%s with %d workers", owner, name, len(whosts))

    def shutdown(self, reason="N/A"):
        self._logger.warning("Shutting down due to reason: '%s'", reason)

        # Shut down the workers
        if hasattr(self, "workers"):
            self._logger.info("Shutting down workers")
            for worker in self.workers:
                runtime.shutdown(remote_node=worker)

        # Shut down the node running the hub and dependency cache
        if hasattr(self, "hub"):
            self._logger.info("Shutting down hub")
            runtime.shutdown(remote_node=self.hub)

        dirs = []
        if hasattr(self, "dep_dir"):
            dirs.append(self.dep_dir)

        if hasattr(self, "scratch_dir"):
            dirs.append(self.scratch_dir)

        if len(dirs) > 0:
            self._logger.debug("Attempting to delete temp dirs")
            # Make a number of attempts to delete the scratch and deps folders
            for _ in xrange(4):
                if all(map(lambda d: try_delete_dir(d, self._logger), dirs)):
                    break

                runtime.sleep(15)

    def _dir_purge_failed(self, directory):
        tb = traceback.format_exc()
        raise ValueError("Session directory %s already exists and could not "
                         "be deleted automatically due to:\n%s\n\n"
                         "Please delete the directory manually or use a different session name." % (tb, directory))


class Cloud(object):
    def __init__(self, name, owner, hub, worker_count, created_on):
        self.name = name
        self.owner = owner
        self.hub = hub
        self.worker_count = worker_count
        self.created_on = created_on

    def apply(self, func, *args, **kwargs):
        return sync(self.hub.apply(func, args, kwargs))

    def map(self, func, sequence, *args, **kwargs):
        results, errors = sync(self.hub.map(func, sequence, args, kwargs))
        if len(errors) > 0:
            raise CompositeError(results, errors)
        return results

    def __enter__(self):
        return self

    def __str__(self):
        return "Cloud(name='%s', owner='%s', worker_count='%d', created_on=%s" \
               % (self.name, self.owner, self.worker_count, self.created_on)


class LocalCloud(Cloud):
    def __init__(self, name, owner, session):
        super(LocalCloud, self).__init__(name, owner, session.hub, len(session.workers), session.created_on)
        self.session = session

    def __exit__(self, *_):
        self.shutdown()

    def shutdown(self):
        self.session.shutdown()


class RemoteCloud(Cloud):
    def __init__(self, name, owner, hub, created_on, worker_count, master):
        super(RemoteCloud, self).__init__(name, owner, hub, worker_count, created_on)
        self.master = master

    def __exit__(self, *_):
        self.shutdown()

    def shutdown(self):
        self.master.shutdown_session(self.name)