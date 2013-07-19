import os
import time
import socket
import shutil
import traceback


def retry(func, limit=5, timeout=60, exceptions=(Exception,), logger=None):
    """
    Retry the supplied function until it succeeds or the retry/timeout limit is exceeded.

    :param func: The function to run.
    :param limit: Optional. Max number of attempts.
    :param timeout: Optional. Max amount of time to take.
    :param exceptions: Optional. Exception types that should trigger a retry.
    :param logger: Optional. Logger to use for pringing out diagnostics.
    :return: The result of func().
    """
    delay = timeout / float(limit)
    attempt = 0
    while True:
        try:
            return func()
        except exceptions as e:
            if logger:
                logger.error("Retry (%s of %s) - error:\n%s" % (attempt + 1, limit, traceback.format_exc()))
            if attempt < limit:
                attempt += 1
                time.sleep(delay)
            else:
                raise e


def try_delete_dir(directory, logger):
    """
    Recursively try to delete the directory. Will try to unlock the directory/file in case it is locked.

    :param directory: Directory to recursively delete.
    :param logger: Logger to use for printing out diagnostics.
    :return: True if the directory was deleted. False otherwise.
    """
    def _on_error(func, path, _):
        import stat

        if not os.access(path, os.W_OK):
            # Is the error an access error ?
            os.chmod(path, stat.S_IWUSR)
            func(path)
        else:
            logger.warn("Failed to delete %s due to %s%s .", path, os.linesep, traceback.format_exc())
            raise

    try:
        shutil.rmtree(directory, onerror=_on_error)
        return True
    except:
        return False


def get_ip():
    """
    :return: The the local IP address
    """
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)