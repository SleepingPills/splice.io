import os
import tempfile
import logging


def setup_defaults(level=None):
    """
    Set up sensible basic logging settings.

    :param level: Optional. Logging level to use.
    :return: The effective level.
    """
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if level:
        logging.getLogger().setLevel(level)

    return logging.getLogger().getEffectiveLevel()


class TempDirFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        super(TempDirFileHandler, self).__init__(os.path.join(tempfile.gettempdir(), filename), mode, encoding, delay)