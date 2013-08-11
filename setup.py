import os
import zmq
import sys
import gevent

from setuptools import setup, Extension
from Cython.Distutils import build_ext

include_dirs = zmq.get_includes()
zmq_lib_dir = None

# Check version requirements
min_gevent_version = (1, 0, 0)

if gevent.version_info < min_gevent_version:
    print("Error: gevent must be version %s or higher, got %s" % (str(min_gevent_version), str(gevent.version_info)))
    sys.exit(1)

# Use any extra include and library locations
for arg in sys.argv[:]:
    if arg.find("--zmq-include=") == 0:
        include_dir = arg[14:]
        include_dirs.append(os.path.join(include_dir))
        sys.argv.remove(arg)
    elif arg.find("--zmq-lib=") == 0:
        if zmq_lib_dir is not None:
            print("Error: multiple instances of `zmq-lib` specified")
            sys.exit(1)

        zmq_lib_dir = os.path.abspath(os.path.expanduser(arg[10:]))
        sys.argv.remove(arg)

# Determine if pyzmq contains a bundled libzmq
if sys.platform.startswith("win"):
    zmq_lib_name = "libzmq"
    zmq_lib_ext = ".dll"
else:
    zmq_lib_name = "zmq"
    zmq_lib_ext = ".dylib" if sys.platform == "darwin" else ".so"

pyzmq_dir = os.path.dirname(zmq.__file__)
zmq_lib_path_bundled = os.path.join(pyzmq_dir, "libzmq" + zmq_lib_ext)

if os.path.exists(zmq_lib_path_bundled):
    # Warn the user in case libzmq dir is explicitly specified but it is not the same as the bundled libzmq
    if zmq_lib_dir is not None and zmq_lib_dir != pyzmq_dir:
        print("Warning: pyzmq is using bundled libzmq, but splice.io will be\n"
              "         linked against instance in directory: %s" % zmq_lib_dir)
    else:
        zmq_lib_dir = pyzmq_dir

# Exit in case there was no explicit libzmq dir specified and it isn't bundled with pyzmq
if zmq_lib_dir is None:
    print("Error: libzmq directory was not specified and a pyzmq-bundled version couldn't be found either")
    sys.exit(1)


library_dirs = [zmq_lib_dir] if zmq_lib_dir is not None else []

setup(
    name="splice.io",
    version="1.0.0",
    description="Distributed processes and coroutines for python.",
    author="Tom Farnbauer",
    author_email="tom.farnbauer@gmail.com",
    packages=["splice",
              "splice.cluster",
              "splice.datastruct",
              "splice.serialization",
              "splice.util"],
    scripts=["bin/sp_run_instance.py", "bin/sp_run_master.py"],
    ext_modules=[Extension("splice.receiver",
                           ["splice/receiver.pyx"],
                           library_dirs=library_dirs,
                           runtime_library_dirs=library_dirs,
                           libraries=[zmq_lib_name])],
    include_dirs=include_dirs,
    install_requires=[
        "gevent >= 1.0dev",
        "pyzmq >= 2.2",
        "psutil",
        "bintrees"],
    cmdclass={"build_ext": build_ext})