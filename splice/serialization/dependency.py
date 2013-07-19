import imp
import re
import os
import tarfile
import sys

from cStringIO import StringIO

suffixes = imp.get_suffixes()


def get_suffix(filename):
    for suffix in suffixes:
        if filename[-len(suffix[0]):] == suffix[0]:
            return suffix
    return None


def getmodules(p):
    # get modules in a given directory
    compiled_rx = re.compile("(?i)[a-z_]\w*$")
    modules = {}
    for f in os.listdir(p):
        f = os.path.join(p, f)
        if os.path.isfile(f):
            m, e = os.path.splitext(f)
            suffix = get_suffix(f)
            if not suffix:
                continue
            m = os.path.basename(m)
            if compiled_rx.match(m):
                if suffix[2] == imp.C_EXTENSION:
                    # check that this extension can be imported
                    try:
                        __import__(m)
                    except ImportError:
                        continue
                modules[m] = f
        elif os.path.isdir(f):
            m = os.path.basename(f)
            if os.path.isfile(os.path.join(f, "node.py")):
                for mm, f in getmodules(f).items():
                    modules[m + "." + mm] = f
    return modules


def getpath():
    path = map(os.path.normcase, map(os.path.abspath, sys.path[:]))

    # get rid of non-existent directories and the current directory
    def cb(p, cwd=os.path.normcase(os.getcwd())):
        return os.path.isdir(p) and p != cwd

    path = filter(cb, path)
    return path


def get_installed_packages():
    path = getpath()

    modules = {}
    for m in sys.builtin_module_names:
        modules[m] = None

    for p in path:
        modules.update(getmodules(p))

    return set([get_package_name(module) for module in modules.keys()])


def get_package_name_of_module(module):
    return get_package_name(module.__name__)


def get_package_name(module_name):
    p_div_idx = module_name.find(".")
    if p_div_idx == -1:
        return module_name

    return module_name[:p_div_idx]


def get_packager(module):
    path_sep = os.path.sep

    name = get_package_name_of_module(module)

    if not os.path.exists(module.__file__):
        package_path = path_sep + path_sep.join(module.__name__.split("."))
        path = module.__file__[:module.__file__.rfind(package_path)]
        return EggPackager(path, os.path.basename(path), name)

    if "." in module.__name__:
        # If this is not a root module, extract the root
        package_path = path_sep + path_sep.join(module.__name__.split(".")[1:])
        path = module.__file__[:module.__file__.rfind(package_path)]
    else:
        path = module.__file__[:module.__file__.rfind(path_sep)]

    return DirectoryPackager(path, name)


class Packager(object):
    def __init__(self, path, name):
        self.path = path
        self.name = name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Packager):
            return self.name == other.name
        else:
            return False


class DirectoryPackager(Packager):
    def __init__(self, path, name):
        super(DirectoryPackager, self).__init__(path, name)

    def package(self):
        # Package stuff only once
        if hasattr(self, "data"):
            return

        buf = StringIO()
        f = tarfile.open(fileobj=buf, dereference=True, mode="w")

        try:
            f.add(self.path, self.name)
        finally:
            f.close()

        self.data = buf.getvalue()

    def deploy(self, path):
        buf = StringIO()
        buf.write(self.data)
        buf.reset()
        f = tarfile.open(fileobj=buf, dereference=True)
        try:
            f.extractall(path)
        finally:
            f.close()


class EggPackager(Packager):
    def __init__(self, path, filename, name):
        super(EggPackager, self).__init__(path, name)
        self.filename = filename

    def package(self):
        # Package stuff only once
        if hasattr(self, "data"):
            return

        with open(self.path, "rb") as f:
            self.data = f.read()

    def deploy(self, path):
        fullpath = os.path.join(path, self.filename)
        with open(fullpath, "wb") as f:
            f.write(self.data)


class DependencyError(Exception):
    def __init__(self, *args, **kwargs):
        super(DependencyError, self).__init__(*args, **kwargs)