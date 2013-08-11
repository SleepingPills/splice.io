=========================================================
splice.io: distributed processes and coroutines in python
=========================================================

What is it
==========
**splice.io** is an attempt at bringing Erlang style distributed processes
(AKA *actors*) and truly parallel coroutines to python. To that end, it
strives to provide an intuitive and easy to use message passing mechanism
and consistent scheduling characteristics.

The API is constructed such that the most common tasks should just
*work* and they should be very simple to achieve. This is not to say
that flexibility should be sacrficed, but that it should not come at the
cost of a massive and complex interface.

To that end, basic splice processes can be thought of as service objects
which can be called via simple python method call syntax (like RPCs).
However, to truly grasp the way **splice.io** works, it is better to think
of the method call syntax as a shorthand for sending messages to named
message handlers. The delivery is handled by a mailbox that sequences
and dispatches the incoming messages.

Disclaimer
==========
**splice.io** is prototype software. It is not yet finished or feature complete
by any means. Many things will not work as expected or not work at all. It is sorely
lacking unit tests. `This cat <http://imgur.com/zGHmZ>`_ was in the vicinity while
the code was being written. This probably explains a lot.

Use at your own risk!

Having said that, the core functionality should work as advertised.

For an idea on what features/changes are planned, take a look at the ``NOTES`` file.

Main Features
=============
 - Construct distributed processes from simple python classes.
 - Construct distributed coroutines from python generators.
 - Communication is asynchronous by default. Sending a message to a process will result
   in a Promise (or Future) that will eventually contain the response (if any). The response
   can be a regular value, or an exception if the recieving process failed.
 - Consistent scheduling. Similarly to how Erlang actors work, a single splice process
   only ever handles one message at a time. Separate processes are allowed to execute
   concurrently (inside the same OS process) or parallel (accross OS processes).
   This guarantee means that it is easier to reason about the interaction between
   processes, but most importantly, there is no need for synchronisation mechanisms
   inside the processes themselves. At the same time, processes are implicitly
   parallel streams so they scale naturally.
 - Splice processes are lightweight. They have a small memory footprint and
   inactive processes do not take up processor time. One can have as many
   processes as memory allows; generally in the order of hundreds of thousands
   to millions.
 - ZMQ is used as a low level transport mechanism providing well known bugs
   and shortcomings, instead of a custom thing with unknown bugs and shortcomings.
 - Robust serialization from the PiCloud http://www.picloud.com/ client library.
 - A small furry kitten is petted and given treats every time you create a splice process.

Reliability
===========
 - **splice.io** guarantees that only whole messages will be dispatched. E.g. either the entire
   message is delivered, or nothing is delivered.
 - **There is no guarantee that a particular message will be delivered.** The underlying ZMQ
   transport does not use durable message queueing and there isn't one implemented in
   **splice.io** either.
   Some (but not all) scenarios under which messages can be lost:
   - In case the host OS process crashes, any unhandled (but received) messages will be lost.
   - In case the internal ZMQ message buffers get filled, additional messages will be silently discarded.
 - An error will be raised in case message delivery cannot be confirmed. This is a mechanism
   provided by **splice.io** on top of ZMQ. Note that ZMQ itself does not provide any notification
   in case of delivery failures.
 - **splice.io** will attempt to reconnect if a connection gets dropped, and messages
   will be buffered up to a certain limit (this will be configurable in the future).
 - **splice.io** will start transmitting as soon as a connection is established. If A connects
   to B, but B is not running yet, mesages will be buffered and then transmitted as soon as
   B comes online.

Performance
===========
There has been next to no performance tuning done yet, but there was an effort to avoid
unneccessary overheads. To get the best performance, it is advised to use asynchronous
message passing whenever possible. Synchronous communication is not only slower, but can result in
deadlocks if one is not careful; a simple example is when process A is waiting for
a response from process B, and process B is waiting for a response from process A.

Building & Installation
============
Building and installation requires the ZMQ headers and library files to be available.
Once those are acquired, simply run::

    setup.py install --zmq=<dir containing an `include` and `lib` folder>

Examples
========
The most frequently used classes are exposed in the root ``splice`` namespace::

    from splice import runtime, Process, ProcessMeta, sync, syncmany

Designate a python class as a splice process
--------------------------------------------
To turn a class into a splice process, simply add the ``splice.ProcessMeta`` metaclass.
``splice.ProcessMeta`` will add ``splice.Process`` to the parent class list in case
it is not specified. For the sake of clarity, it is best to do so explicitly::

    class MyProc(Process):
        \_\_metaclass\_\_ = ProcessMeta

        def some_handler(some_param):
            return some_param

Spawn a splice process instance locally
---------------------------------------
The ``spawn`` method of the runtime handles process creation. Positional and keyword
arguments can be passed after the type is specified::

    proc = runtime.spawn(MyProc, arg1, kwarg1="moof")

Spawn a splice process instance on a remote node
------------------------------------------------
``spawn`` takes a number of special keyword arguments (prefixed with ``sp_``), one
of these is the destination address.::

    proc = runtime.spawn(MyProc, sp_dest="tcp://10.1.1.15:54321")

The handle returned for a process spawned remotely is a transparent proxy that
relays all calls to the remote object.

Connect to an existing process
------------------------------
Predictably, the ``connect`` method connects to remote (or local) processes.
The address is specified as a ``splice.uri.url`` object::

    proc = runtime.connect(splice.uri.url("my_proc", "tcp://10.1.1.15:54321"))

Send a message to a process
---------------------------
Sending messages is dead simple, one just needs to call methods on
the receiving process\:::

    result = proc.some_handler("Hello World!")

The return value is not the result produced by the process (since message passing
is asynchronous), but a Promise object that will eventually contain the result.

To retrieve the actual value, one can call the ``get()`` method::

    result.get()

The better way to do it is to use ``sync`` function, which can wrap the method
call directly::

    result = sync(proc.some_handler("Hello World!"))

Note that the result does not need to be evaluated immediately, it can happen
at any time after the message has been sent. In case the response has already
arrived, both the ``sync`` function and the ``get`` method will return immediately.

``syncmany`` is a shorthand for synchronously evaluating many Promises at once::

    results = syncmany(promises)

Scatter splice processes across many nodes
------------------------------------------
``spawn_scatter`` can be used to spawn a particular process on a list of nodes::

    procs = runtime.spawn_scatter(destinations, MyProc)

Spawn a coroutine
-----------------
``Coroutines`` are simple splice processes that wrap a python generator::

    def some_coro(arg1, arg2):
        acc = arg1
        for _ in range(10):
            yield acc
            acc += arg2

    coro = runtime.spawn(some_coro, 10, 5)

Splice coroutines have practically the same semantics as python generators, so
one can iterate over them (NOTE: iteration is inherently synchronous!)::

    for item in coro:
        print item

Coroutines also support bidirectional communication. E.g. in the below
example, the coroutine first yields the currently accumulated value, and
then waits for a new value to arrive. It then adds the new value to the
accumulator variable::

    def some_coro():
        acc = 0
        for _ in range(10):
            acc += yield acc

    coro = runtime.spawn(some_coro)

    # Coroutines are asynchronous by default as well
    promise = coro.send(5)

    # One can use `sync`
    result = sync(coro.send(5))


Send splice process references
------------------------------
Splice processes get pickled as a simple proxy object, so they can be easily
transmitted as arguments or even fields of nested objects. The recieving end
will get a transparent proxy that relays messages to the host node::

    # Send a reference of the coroutine to a remote process
    proc.some_handler(coro)

Fork a splice node
------------------
The ``runtime.fork`` method can be used to quickly spin up a number of nodes
that can host splice processes on the local machine::

    nodes = runtime.fork(10) # Spawn 10 nodes (OS processes)

Currently this method
simply returns a list of addresses that can be fed to ``runtime.spawn_scatter``, but
in the future there will be a more robust ``view`` mechanism on remote nodes.

Stop a splice process
---------------------
Splice processes can be easily stopped::

    runtime.stop(proc) # `proc` can be a process reference or a proxy

Shut down a splice node
-----------------------
To gracefully shut down a splice node, it is best to use the ``runtime.shutdown``
method. This method can shut down remote nodes and ensures that all child nodes are
properly cleaned up::

    runtime.shutdown() # Terminate the local node
    runtime.shutdown(remote_node=some_remote_address) # Terminate a remote node

Cluster examples
================
As a proof of concept, **splice.io** contains a small yet easy to use cluster
implementation to quickly hook up multiple machines and farm out lots of work.

The cluster implementation is still work in progress, but should be sufficient for basic
parallel computing needs.

Current characteristics:

 - Full session isolation. Workers live only as long as the session and are not
   shared. Once the session ends, worker processes are terminated.
 - Automatic and transparent dependency sharing
 - Support for simple parallel computations
 - Naive load balancing

Dependency handling
-------------------
Perhaps the most interesting feature of the **splice.io** cluster is that
there is no need for nodes to share a single python runtime, or that python dependencies
be preinstalled (as is the case with beowulf clusters). Splice cluster nodes will
automatically download missing dependencies from the client as required.

In practice, this works very well with some restrictions, e.g. worker nodes and
the client machine need to be of the same architecture for extension module
dependencies to work. Pure python modules will work as long as the python runtimes
between the client and the workers are compatible.

The dependency management happens transparently to the user. Whenever an module import
fails on a worker node, it will ask the client if that module is available there. In case
it is, the worker node downloads the dependency into a temporary session storage.

Start a local cluster
---------------------
It is dead simple to fire up a cluster. One just needs to import some
plumbing from ``splice.cluster``::

    from splice.cluster import session
    c = session(worker_count=4) # Omitting `worker_count` will result in using all CPUs

The cluster session also works as a ContextManager, so it will clean up
all worker processes once the work is finished\:::

    with session() as c:
        <do work>

Farm out work
-------------
With the cluster session at hand, farming out work is again dead simple::

    results = c.map(lambda v: v ** 2, range(100))

Extra function arguments can be provided as well\:::

    results = c.map(lambda v, a: v ** a, range(100), 5)

The ``apply`` method can be used to simply execute a task on all workers::

    def work(arg, named_arg=None)
        ...

    c.apply(work, 5, named_arg=10)

Observant individuals will note that **splice.io** works with lambda functions as
well, which is a feature not supported by standard python multiprocessing. Indeed, thanks
to the awesome PiCloud serializer, **splice.io** fully supports closures!::

    closed_var = 5

    def work(input):
        return input * closed_var

    results = c.map(work, range(10))

Setting up a managed cluster
----------------------------
To run a cluster accross multiple machines, one needs to set up a master node
and then initialize worker node instances such that they connect to the master.

The master node can be set up using the ``sp_run_master.py`` script::

    sp_run_master.py --port 50000

Assuming the master node machine name is *master-node*, worker nodes can be run
thusly::

    sp_run_instance.py --master-address tcp://master-node:50000

Once the instances register with the master node, they can start accepting work.

To use the newly set up cluster, one just has to add the master address as a
parameter to the session::

    c = session(worker_count=20, master_address="tcp://master-node:50000")

