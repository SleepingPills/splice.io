import tempfile
import multiprocessing as mp
import splice.util.log as spl

from splice import runtime, sync
from splice.uri import uid, url
from splice.cluster.infra import DependencyServer, Session, LocalCloud

# Call logging setup only once. The builtin logging infrastructure also ensures
# that initialization happens only once, but it uses thread synchronisation which
# we can avoid in most cases.
_log_setup = False


def session(name=None, owner="anon", worker_count=None, work_dir=None, master_address=None, init=None):
    """
    Creates a new cluster session.

    The returned object can be used to execute work on the cluster:

    >>> c = session() # Creates a cluster on the local machine with one worker per core.
    >>> print c.apply(lambda: 5) # Returns 5 from each worker
        [5, 5, 5 ,5]

    The cluser session can be used as a context manager as well, in which case all
    resources will be cleaned up and released after exiting the `with` clause:

    >>> with session() as c:
    ...     print c.map(lambda i: i*2, [1,2,3,4])
        [2, 4, 6 ,8]
        Session(a73a6153-2295-4d66-90ad-be8e0bb1d56d,anon):Shutting down

    :param name: Optional name of the session. Relevant only when registering with a master.
    :param owner: Optional owner of the session. Relevant only when registering with a master.
    :param worker_count: The number of workers to request. The default is the currently
                         available capacity (e.g. the # of processors in case of a local
                         cluster).
    :param work_dir: Working directory to use for storing temporary data. Required when
                     registering with a master. For local clusters, the temp directory
                     will be used by default.
    :param master_address: When specified, a session will be created with the master
                           node, enabling the usage of dedicated clusters.
    :param init: A custom initialization function that will be run on the workers
                 after instantiation.
    :return: A session that can be used to communicate with the cluster.
    """
    global _log_setup

    if not _log_setup:
        spl.setup_defaults()
        _log_setup = True

    if name is None:
        name = uid()

    # Spawn dependency server
    dep_server = runtime.spawn(DependencyServer)

    # If master address is none, create a local cloud
    if master_address is None:
        if work_dir is None:
            work_dir = tempfile.gettempdir()

        if worker_count is None:
            worker_count = mp.cpu_count()

        session = Session(name, owner, dep_server, worker_count, _create_local_workers, work_dir, init)
        return LocalCloud(name, owner, session)

    master = runtime.connect(url("splice-master", master_address))

    return sync(master.get_session(name, owner=owner, dep_server=dep_server,
                                   work_dir=work_dir, worker_count=worker_count, init=init))


def _create_local_workers(name, owner, worker_count, init):
    return runtime.fork(worker_count, init)
