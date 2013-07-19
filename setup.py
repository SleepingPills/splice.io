import os
import zmq
import sys

from setuptools import setup, Extension
from Cython.Distutils import build_ext

include_dirs = zmq.get_includes()
library_dirs = []

# Use any extra include and library locations
for arg in sys.argv[:]:
    if arg.find("--zmq=") == 0:
        zmq_dir = arg[6:]
        include_dirs.append(os.path.join(zmq_dir, "include"))
        library_dirs.append(os.path.join(zmq_dir, "lib"))
        sys.argv.remove(arg)
        print "zmq dir:", zmq_dir

zmq_lib = "libzmq" if sys.platform.startswith("win") else "zmq"

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
    ext_modules=[Extension("splice.reciever",
                           ["splice/reciever.pyx"],
                           library_dirs=library_dirs,
                           libraries=[zmq_lib])],
    include_dirs=include_dirs,
    install_requires=[
        "gevent >= 1.0dev",
        "pyzmq >= 2.2",
        "psutil",
        "bintrees"],
    cmdclass={"build_ext": build_ext})