import os
import sys
import master
import psutil
import logging
import tempfile
import traceback

import splice.util.log as spl

from splice import sync, runtime, ProcessMeta
from splice.uri import uid
from splice.util import try_delete_dir


class Instance(object):
    __metaclass__ = ProcessMeta

    def __init__(self, master, keepalive_timeout):
        self._logger = logging.getLogger(self.__class__.__name__)

        # Set up working directory
        tmp_dir = os.path.join(tempfile.gettempdir(), "splice_node")
        if os.path.exists(tmp_dir):
            # Delete all leftover cruft from previous sessions
            try_delete_dir(tmp_dir, self._logger)

        os.mkdir(tmp_dir)
        self._tmp_dir = tmp_dir
        with open(os.path.join(tmp_dir, "$splice_node$"), "wb") as f:
            f.write(uid("node"))

        self._master = master
        self._keepalive_timout = keepalive_timeout
        self._load = 0

        self._logger.info("Starting keepalive loop.")
        self._keepalive_timer = runtime.greenpool.spawn(self._master_keepalive)

    def _master_keepalive(self):
        retries = 0
        while True:
            ram_total, _, _, ram_percent = psutil.phymem_usage()

            try:
                self._logger.debug("Pinging master with usage stats.")
                sync(self._master.node_update(self,
                                              psutil.NUM_CPUS,
                                              psutil.cpu_percent(),
                                              ram_total / 1e9,
                                              ram_percent / 1e2),
                     timeout=self._keepalive_timout)
            except BaseException:
                if retries >= 5:
                    self._logger.error("Master node unreachable, shutting down:\n%s" % traceback.format_exc())
                    self.shutdown()
                else:
                    self._logger.error("Error pinging master node:\n%s" % traceback.format_exc())
                    retries += 1

            runtime.sleep(10)

    def spawn_workers(self, worker_count, init=None):
        return runtime.fork(worker_count, init=init)

    def shutdown(self):
        self._logger.info("Shutting down monitor.")
        self._keepalive_timer.kill()
        runtime.shutdown()


def run_instance(master_address, log_level=None, keepalive_timeout=120):
    spl.setup_defaults(log_level)
    _logger = logging.getLogger(__name__)

    master_proc = master.connect(master_address)
    _logger.info("Connected to master at " + str(master_proc.sp_url))

    runtime.spawn(Instance, master_proc, keepalive_timeout)

    _logger.info("Cluster node online")
    runtime.start()
    _logger.warn("Exiting node process")
    sys.exit()