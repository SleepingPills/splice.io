try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import cloudpickle


def dumps(obj):
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def dump(obj, f):
    pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)


def dumps_robust(obj):
    stream = StringIO()
    dump_robust(obj, stream)
    return stream.getvalue()


def dump_robust(obj, f):
    pickler = cloudpickle.CloudPickler(f, pickle.HIGHEST_PROTOCOL)
    pickler.dump(obj)


def load(f):
    return pickle.load(f)


def loads(data):
    return pickle.loads(data)
