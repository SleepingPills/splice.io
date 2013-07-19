from uuid import uuid4
from collections import namedtuple

url = namedtuple("url", "resource address")


def uid(prefix=None):
    pid = str(uuid4())
    return "%s-%s" % (prefix, pid) if prefix else pid