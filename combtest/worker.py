"""
This module provides a set of mechanisms for dispatching "work" across a
set of worker nodes. "Work" can mean "execute sets of arbitrary Python
code". This system is made of a few pieces:

* A basic thread pool implementation. There is a :class:`multiprocessing.ThreadPool`
  implementation in Python already,
  but I want the control and flexibility of having my own. This and its
  decendents are the "workers" that actually e.g. execute test cases or
  run fuzz+stress load (see :class:`ThreadPool`). Thread
  counts can be chosen by the client, by default, whatever.
* An rpyc-based coordinator that deserializes work sent to it and sends it
  to a worker/executor of some sort, which will typically be a ThreadPool
  (see :class:`CoordinatorService` below). Typically there will be one of
  these per node running in its own process, or at least one of a given
  "type" per node. The only constraint is that it needs to have a 1:1 ip/port
  mapping. Typically there will be one central coordinator
  (:class:`ServiceGroup`) paired with one ``CoordinatorService`` per node.
* A central coordinator running in a single process that is passed work and
  sends it to all paired remote coordinators for execution
  (:class:`ServiceGroup`).

Work is sent in some quantum defined by the client code and passed from
``ServiceGroup->CoordinatorService->ThreadPool``. Responses can be sent back.

* The quantum should be large enough for a worker to keep its children busy
* The child may quit before all work is done if e.g. a failure limit is
  reached, or a fatal error hit.
* A parent can specify how many workers a child can have, otherwise a worker
  is free to decide.
* A parent can raise signal or other condition to a child to tell it to quit
  early. It must obey and send back any accumulated responses, or have its
  results abandoned.
* Parent must deal with errors where the child dies silently (e.g.
  interpreter crash hard enough that we don't run atexit handlers)

"""
import copy
import rpyc
import signal
import sys
import threading
import time
import traceback

import combtest.bootstrap as bootstrap
from combtest.config import refresh_cfg, get_ssh_options, get_machine_ips, \
        get_service_port, set_service_port, get_max_thread_count
import combtest.central_logger as central_logger
from combtest.central_logger import logger
import combtest.config as config
import combtest.encode as encode
import combtest.utils as utils


# We are going to force requests and responses to be pickled. The reason is
# that netref presents a potential performance landmine, and since we allow the
# user to send arbitrary stuff and do arbitrary things with it, that is not
# something we can control. Instead, lets force them to send things in pickled
# form, which will cut the netrefs and do an actual copy.
rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True


def _ensure_stderr_handler():
    # Root logger will log issues to stderr
    root_logger = central_logger.logging.getLogger()
    sh_stderr = central_logger.logging.StreamHandler(sys.stderr)
    root_logger.addHandler(sh_stderr)


class ThreadPool(object):
    """
    A set of threads doing work from a queue asynchronously to the caller. The
    caller can add additional work after the work has started. Child classes
    can add callbacks for e.g. when work starts/finishes, how to handle errors,
    etc.
    """
    def __init__(self, max_thread_count=None, work=None, **kwargs):
        """
        :param int max_thread_count: max number of threads to have running at
                                     a time
        :param iterable work: a pop-able iterable of work items. Work
                              items are callable objects that take 0
                              arguments unless a ``ctx`` is supplied to
                              :func:`start`, in which case they take 1
                              argument.
        """
        self._thread_count = max_thread_count or get_max_thread_count()
        self._work_queue = work or []
        self._run_ctx = None
        self._running_threads = []
        self.should_stop = False

        self._working = 0
        self._rlock = threading.RLock()

    def add_work(self, work):
        """
        Add a work item to this pool. Will not kick threads into action if all
        prior work has finished. See :func:`kick_workers`.
        """
        self._work_queue.extend(list(work))

    def on_start(self, ctx):
        """
        Called when threads start up to do work for the first time. Not re-run
        when they are "kicked".
        """
        pass

    def on_finish(self):
        """
        Called when threads are finished up, as indicated by a call to
        :func:`join`.
        """
        pass

    def on_start_item(self, work_item, ctx):
        """
        Called after a work item is dequeued and before it is run.
        """
        pass

    def on_finish_item(self, work_item, ctx):
        """
        Called after a work item is finished running.
        """
        pass

    def on_error_item(self, work_item, ctx, exc, tb):
        """
        Called if a work item errors out (via Exception).
        """
        raise

    def _run_single(self, work_item, ctx):
        """
        Called by a worker thread to run a work item.
        """
        try:
            self.on_start_item(work_item, ctx)
            if ctx is not None:
                work_item(ctx)
            else:
                work_item()
            self.on_finish_item(work_item, ctx)
        except Exception as exc:
            tb = traceback.format_exc(sys.exc_info())
            self.on_error_item(work_item, ctx, exc, tb)

    def _thread_main(self, ctx, single=None):
        """
        Entry point for worker threads.
        """
        with self._rlock:
            self._working +=1

        try:
            if single is not None:
                self._run_single(single, ctx)

            while self._work_queue and not self.should_stop:
                try:
                    work_item = self._work_queue.pop()
                except IndexError:
                    return

                start_time = time.time()
                self._run_single(work_item, ctx)
                ## Left intentionally for future debugging
                #logger.debug("Work item took %0.2fs", time.time() - start_time)
        finally:
            with self._rlock:
                self._working +=1

    def signal_stop(self):
        """
        Signal threads they should stop when they are done with their current
        unit of work. This means they will not stop as quickly as e.g. killing
        this proc with a signal. Gives them a chance to finish cleanly.
        """
        self.should_stop = True

    def start(self, ctx=None):
        """
        Tell threads to start working on any work already queued.

        :param object ctx: an optional single arg to pass to the work callbacks
        """
        self._run_ctx = ctx
        self.should_stop = False

        self.on_start(ctx)

        # We could dynamically scale up/down based on the size of the work
        # queue, but let's assume for now that the user won't use a ThreadPool
        # unless there is an appreciable amount of work, and that they are
        # providing it fast enough to busy the threads. Keeps things simple.
        while len(self._running_threads) < self._thread_count:
            t = threading.Thread(target=self._thread_main,
                    args=(self._run_ctx,))
            t.start()
            self._running_threads.append(t)

    def start_all_on_next(self, ctx=None):
        """
        This gets all threads running the next work item. This means the item
        will potentially be run more than once, and potentially multiple times
        in parallel.

        :param object ctx: an optional single arg to pass to the work callbacks
        """
        self._run_ctx = ctx
        self.should_stop = False

        self.on_start(ctx)
        work_item = self._work_queue.pop(0)
        while len(self._running_threads) < self._thread_count:
            t = threading.Thread(target=self._run_single, args=(work_item,
                self._run_ctx))
            t.start()
            self._running_threads.append(t)

    def kick_workers(self):
        """
        If some workers are dead, reap them and start up new ones to replace
        them. This is useful e.g. when adding work after the pool is already
        running, since some may have finished and exited.
        """
        new_running_threads = []
        for thread in self._running_threads:
            # Yes this is racy in favor of keeping threads that may die a
            # moment later.
            if thread.is_alive():
                new_running_threads.append(thread)
            else:
                new_thread = threading.Thread(target=self._thread_main,
                        args=(self._run_ctx,))
                new_thread.start()
                new_running_threads.append(new_thread)

        self._running_threads = new_running_threads


    def join(self):
        """
        Wait for all threads to finish, then call on_finish.
        """
        while self._running_threads:
            t = self._running_threads.pop(0)
            t.join()

        self.on_finish()

    def run(self, ctx=None, all_on_next=False):
        """
        Run all queued work all the way through to completion, then join.

        :param object ctx: an optional single arg to pass to the work callbacks
        :param bool all_on_next: rather than each thread popping an item that
                                 it runs (and therefore no other thread runs),
                                 pop a single item and have all threads run it.
        """
        # Synchronous interface
        if all_on_next:
            self.start_all_on_next(ctx=ctx)
        else:
            self.start(ctx=ctx)

        self.join()


class CoordinatorService(rpyc.Service):
    """
    Receives work from a remote process via rpyc, starts the work running via
    :class:`ThreadPool` or some decendent class.
    """
    #: a ThreadPool class that workers should be instantiated from
    #: User can override
    WORKER_TYPE = ThreadPool

    #: Override point for max number of threads for each worker to have. The
    #: other ways to set this are 1. via config.py, and 2. via an option passed
    #: at runtime (e.g. via :func:`exposed_start_workers_on`).
    DEFAULT_MAX_THREAD_COUNT = get_max_thread_count()

    def on_connect(self):
        """
        :class:`rpyc.Service` is designed for this to be the de-facto
        ``__init__`` function.  So we are doing our initialization here.
        """
        # Maps integer id->worker
        self._workers = {}
        # Maps same integer id->run ctx
        self._ctxs = {}
        self._next_worker_id = 0
        self.rlock = threading.RLock()
        # Maps same integer id->resp
        self._resps = {}

    def _get_next_worker_id(self):
        """
        Get a serial id for the worker. The remote user can use this to refer
        back to the worker.
        """
        with self.rlock:
            next_id = self._next_worker_id
            self._next_worker_id += 1
        return next_id

    # (the rlock may be superfluous here, but I fear for iterators like in
    # join_all if we don't take it)
    def _get_worker_by_id(self, worker_id):
        """
        Returns the worker of the given id, or raises an IndexError if the
        worker does not exist.
        """
        with self.rlock:
            try:
                return self._workers[worker_id]
            except IndexError:
                raise IndexError("No known worker with id %d" % worker_id)

    def _set_worker_by_id(self, worker_id, worker):
        with self.rlock:
            self._workers[worker_id] = worker

    def _del_worker_by_id(self, worker_id):
        with self.rlock:
            del self._workers[worker_id]

    def work_repack(self, work, ctx=None, resp=None):
        """
        This is called back to repackage each work item sent to the service.
        The call is an opportunity to e.g. do some deserialization, wrap the
        ``Walk`` in a ``WalkRunner``, or anything else the user needs to prep
        the work for execution.

        :param object work: the work to execute; typically this will be e.g. a
                            JSONified ``Walk``.
        :param object ctx: a ``ctx`` copied in for executing the ``Walk``
        :param dict resp: a dict to which response objects can be attached by
                          the ``Walk`` for retrieval later.
        """
        return work

    def exposed_start_work(self, work, max_thread_count=None, ctx=None):
        """
        Called by remote side to start some work running asynchronously. The
        work and ctx are pickled and pulled through to this side in whole,
        rather than accessing via netrefs.

        :return: a worker_id to refer back to this work later for e.g. stopping
                 it or getting responses.
        """
        if max_thread_count is None:
            max_thread_count = self.DEFAULT_MAX_THREAD_COUNT

        worker_id = self._get_next_worker_id()
        resp = {}
        self._resps[worker_id] = resp
        work = rpyc.utils.classic.obtain(work)
        ctx = rpyc.utils.classic.obtain(ctx)

        if ctx is None:
            ctx = {}

        packed_work = []
        for work_item in work:
            materialized = rpyc.utils.classic.obtain(work_item)
            packed_work.append(self.work_repack(materialized, ctx=ctx,
                resp=resp))

        worker = self.WORKER_TYPE(max_thread_count=max_thread_count,
                work=packed_work, static_ctx=ctx, resp=resp)
        self._set_worker_by_id(worker_id, worker)
        self._ctxs[worker_id] = ctx
        worker.start(ctx=ctx)

        central_logger.log_status("Started worker with id %d and type %s",
                worker_id, str(type(worker)))


        return worker_id

    def exposed_add_work(self, worker_id, work):
        """
        Add work to an existing worker. The original ctx will be used.
        If the user wants work to be executed with a different ctx,
        they start a new worker up via :func:`exposed_start_work`.

        :param int worker_id: the ``worker_id`` returned from the func used to
                              start work
        :param object work: the work item
        """
        work = rpyc.utils.classic.obtain(work)
        work = [self.work_repack(work_item,
                                 ctx=self._ctxs[worker_id],
                                 resp=self._resps[worker_id])
                for work_item in work]
        worker = self._get_worker_by_id(worker_id)

        central_logger.log_status("Added %d items to worker with id %d",
                len(work), worker_id)

        worker.add_work(work)
        worker.kick_workers()

    def exposed_start_workers_on(self, work, max_thread_count=None, ctx=None):
        """
        Similar to start_work, but a single item is sent, and all threads in
        the new worker will run that single item.

        :return: The ``worker_id`` of the worker executing the work
        """
        if max_thread_count is None:
            max_thread_count = self.DEFAULT_MAX_THREAD_COUNT
        worker_id = self._get_next_worker_id()
        resp = {}
        self._resps[worker_id] = resp
        work = rpyc.utils.classic.obtain(work)
        ctx = rpyc.utils.classic.obtain(ctx)

        materialized = rpyc.utils.classic.obtain(work)
        packed_work = self.work_repack(materialized, ctx=ctx, resp=resp)
        worker = self.WORKER_TYPE(max_thread_count=max_thread_count,
                work=[packed_work], static_ctx=ctx, resp=resp)
        self._set_worker_by_id(worker_id, worker)
        self._ctxs[worker_id] = ctx
        worker.start_all_on_next(ctx=ctx)

        logger.debug("Started worker with id %d and type %s", worker_id,
                str(type(worker)))

        return worker_id

    def exposed_run(self, work, max_thread_count=None, ctx=None):
        """
        Run work synchronously. Return a ``worker_id`` so that responses/ctxs
        can be reclaimed.
        """
        if max_thread_count is None:
            max_thread_count = self.DEFAULT_MAX_THREAD_COUNT
        worker_id = self._get_next_worker_id()
        resp = {}
        self._resps[worker_id] = resp
        work = rpyc.utils.classic.obtain(work)
        ctx = rpyc.utils.classic.obtain(ctx)

        packed_work = []
        for work_item in work:
            materialized = rpyc.utils.classic.obtain(work_item)
            packed_work.append(self.work_repack(materialized, ctx=ctx,
                resp=resp))

        worker = self.WORKER_TYPE(max_thread_count=max_thread_count,
                work=packed_work, static_ctx=ctx, resp=resp)
        self._set_worker_by_id(worker_id, worker)
        self._ctxs[worker_id] = ctx

        logger.debug("Running worker with id %d and type %s", worker_id,
                str(type(worker)))

        worker.run(ctx=ctx)
        self._del_worker_by_id(worker_id)
        return worker_id

    def exposed_start_remote_logging(self, ip, port, **kwargs):
        """
        Start sending logs to the log server at the given ip+port.

        :param str ip: hostname or ip where the log server is running
        :param int port: port number where the log server is running
        """
        central_logger.add_socket_handler(ip, port)

    def exposed_signal_stop(self, worker_id):
        """
        Signal that a worker should stop. May not be immediate since this is
        intended to be a "clean" shutdown where workers can finish their
        current work item.

        :param int worker_id: the ``worker_id`` returned from the func used to
                              start work
        """
        worker = self._get_worker_by_id(worker_id)
        worker.signal_stop()

    def _join_worker(self, worker_id):
        logger.debug("Joining worker with id %d", worker_id)

        worker = self._get_worker_by_id(worker_id)
        worker.join()
        self._del_worker_by_id(worker_id)

    def exposed_join_workers(self, worker_id):
        """
        Join all working threads in a worker and toss the worker for further
        use. The responses and ctxs will stay around until the client "cleans"
        the worker state. That means this is *not* a stateless service. The
        user needs to beware of leaking resources.

        :param int worker_id: the ``worker_id`` returned from the func used to
                              start work
        """
        self._join_worker(worker_id)

    def exposed_join_all_workers(self):
        """
        Like join_workers, but tosses all outstanding workers.
        """
        with self.rlock:
            worker_ids = [worker_id for worker_id in self._workers.keys()]

        for worker_id in worker_ids:
            self._join_worker(worker_id)

    def exposed_gather_ctx(self, worker_id):
        """
        Return the ctx associated with the given worker.

        :param int worker_id: the ``worker_id`` returned from the func used to
                              start work
        """
        return self._ctxs[worker_id]

    def exposed_gather_resp(self, worker_id):
        """
        Return the resp object associated with the given worker.

        :param int worker_id: the ``worker_id`` returned from the func used to
                              start work
        """
        return self._resps[worker_id]

    def exposed_clean_worker(self, worker_id):
        """
        Toss all state related to a worker. Implies a join first.
        Gives the user full control over when we reap memory, since they may
        still want to retrieve/pickle results. It isn't enough to wait until
        after they gather the response, since a.) they may not have "obtained"
        the response value yet, and b.) they may just be gathering intermediate
        results.
        """
        self._join_worker(worker_id)
        del self._resps[worker_id]
        del self._ctxs[worker_id]



# Called from e.g. an ssh session/array using python -c or some such
def start_service(service_class, port=None):
    """
    Start an rpyc service given by the provided class. Port can be overridden.

    :param rpyc.Service service_class: a child class of :class:`rpyc.Service`.
    :param int port: the port the service should listen for requests on. If it
                     isn't provided by the caller, we we get a value from
                     :class:`combtest.config`
    :return: a handle to the resulting :class:`ThreadedServer`.
    """
    # rpyc uses logging, and we want to dump its logging somehow on errors
    _ensure_stderr_handler()

    if port is None:
        port = get_service_port()
    else:
        set_service_port(port)

    from rpyc.utils.server import ThreadPoolServer
    t = ThreadPoolServer(service_class, port=port)
    t.start()
    return t

def start_service_by_name(service_name, port=None):
    """
    Start an rpyc service given by the provided class qualname.
    Port can be overridden.

    :param str service_name: a qualname of achild class of
                             :class:`rpyc.Service`.
    :param int port: the port the service should listen for requests on. If it
                     isn't provided by the caller, we we get a value from
                     :class:`combtest.config`
    :return: a handle to the resulting :class:`ThreadedServer`.
    """
    # rpyc uses logging, and we want to dump its logging somehow on errors
    _ensure_stderr_handler()

    if port is None:
        port = get_service_port()
    else:
        set_service_port(port)

    service_class = utils.get_class_from_qualname(service_name)

    t = start_service(service_class, port=port)
    return t


class ServiceGroup(object):
    """
    Handles to running service instances across the nodes via e.g. SSH, and
    rpyc-based clients connected to those. This has a number of functions for
    dispatching work to the execution services, and gathering responses and
    stats.

    .. warning:: We spawn connections to all nodes. b/w the SSH connections and
                 client connections we may run into scalability issues on giant
                 clusters. There are obvious optimizations we can do here,
                 such as lazy connecting, but it would require some extra
                 hooks. Please file a bug later when/if we prove there is
                 such a problem and a need for a fix.

    """
    INSTANCES = set()

    #: Max time we spend waiting for a service spawn to succeed. We use this
    #: since e.g. starting services does not mean the service is immediately
    #: available, due to waiting for imports to happen. Hence the clients
    #: need to retry for a bit until the services come up. This timeout is
    #: measured in seconds.
    SPAWN_TIMEOUT = 30

    #: The maximum chunk of work we will send in a single RPC call. This is to
    #: keep the request size sane. If the user gives us an iterable that is
    #: generating a massive amount of work on-the-fly we may not be able to
    #: instantiate it all and hold it in memory. So we send batches that are
    #: this size at maximum.
    WORK_QUANTUM_SIZE = 1000

    #: If the user does not provide a specific enumeration of ip/port where we
    #: should set services up running, this will be the default number of
    #: services to start up, which we will start locally.
    DEFAULT_INSTANCE_COUNT = 3

    def __init__(self,
                 service_name,
                 service_infos=None,
                 service_handler_class=bootstrap.ServiceHandler_Local,
                 spawn_services=True,
                 spawn_clients=True):
        """
        :param str service_name: a qualname of the :class:`CoordinatorService`
                                 type to start
        :param iterable service_infos: an iterable of service bootstrapping
                                       info that we will use to bootstrap the
                                       services. For SSH for example this will
                                       be authentication information. See
                                       :class:`bootstrap.ConnectionInfo`.
        :param class service_handler_class: a decendent of
                                            :class:`ServiceHandler`. This is
                                            the type of ``ServiceHandler`` we
                                            will use to bootstrap services.
        :param bool spawn_services: True if we should spawn the remote services
        :param bool spawn_clients: True if we should spawn our local client
                                   connections to the remote services
        """
        self._give_up = False
        ServiceGroup.INSTANCES.add(self)

        # service_infos describe how we bootstrap the remote service which
        #   will actually run the tests (CoordinatorService instances).
        #   Example: a bootstrap_info should be ssh creds/keys if we
        #   are using an SSH-based ServiceHandler.
        self._service_infos = copy.copy(service_infos)
        self._service_handler_class = service_handler_class

        self.service_name = service_name

        # Will be ServiceHandlers once we spawn
        self.service_handles = None
        # Will be a dict mapping (ip, port)->rpyc.connect instances
        self._clients = tuple()

        if spawn_services:
            self.spawn_services()

        # Services and clients are independent since e.g. something else may
        # have started the services already, and we are just connecting.
        if spawn_clients:
            self.spawn_clients()

    @classmethod
    def give_up(cls):
        """
        Signal that all :class:`ServiceGroup` should give up trying to connect
        to remote services, send work, etc. and bail immediately. We can use
        this e.g. if we receive a signal locally and don't want to wait for a
        long-running scatter or gather.
        """
        for instance in cls.INSTANCES:
            instance._give_up = True

    def spawn_services(self):
        """
        Spawn remote services via some bootstrap method.
        """
        assert not self.service_handles, "ERROR: services already running; " \
                "shut them down before spawning again"

        if self._service_infos is None:
            self._service_infos = []

            my_ip = utils.get_my_IP()
            start_port = config.get_service_port()

            for port in range(start_port, start_port +
                    self.DEFAULT_INSTANCE_COUNT):
                ci = bootstrap.ConnectionInfo(my_ip, port, None)
                self._service_infos.append(ci)

        bootstrap.ServiceHandleArray.REMOTE_CONNECTION_CLASS = \
                self._service_handler_class
        self.service_handles = \
                bootstrap.ServiceHandleArray(self._service_infos)

        logger.debug("Attempting to start services of type %s",
                self.service_name)
        self.service_handles.start_cmd(self.service_name)

        logger.debug("Signaled services should start")

    def spawn_clients(self):
        """
        Spawn rpyc clients to the remote services. Assumes remote services are
        up, or will be within ``SPAWN_TIMEOUT``.
        """
        # Retry until all services are up, up to X seconds or whatever. This is
        # just heuristic, so we can be a little sloppy about accounting here.
        self._clients = {}
        for si in self._service_infos:
            ip = si.ip
            port = si.port
            start_time = time.time()
            while (time.time() - start_time) < self.SPAWN_TIMEOUT and \
                   not self._give_up:

                try:
                    client = rpyc.connect(ip, port=port)
                    self._clients[(ip, port)] = client
                    break
                except Exception as e:
                    time.sleep(0.5)

        assert all([(si.ip, si.port) in self.clients for si in
                self._service_infos]), "ERROR: not " \
                "all clients could connect. %s" % str(self._clients)

    def spawn(self):
        """
        Spawn both services and clients (in that order).
        """
        self.spawn_services()
        self.spawn_clients()

    def __getitem__(self, key):
        """
        Return the rpyc client given by an IP.
        """
        if not isinstance(key, basestring):
            raise IndexError("We only support indexing a single element by IP")
        return self._clients[key].root

    @property
    def clients(self):
        """
        Returns a mapping (ip, port)->client "root" (see rpyc connect docs)
        """
        out = {}
        if self._clients is not None:
            for key, client in self._clients.iteritems():
                out[key] = client.root
        return out

    def shutdown_clients(self, hard=False):
        """
        Shut down all running clients.

        :param bool hard: if hard, we will ignore any errors trying to shut
                          down
        """
        if not hard:
            assert self.clients, "ERROR: must spawn clients before shutting " \
                    "them down"

        if self._clients:
            for client in self._clients.values():
                client.close()
        self._clients = None

    def shutdown_services(self, hard=False):
        """
        Shut down all running services.

        :param bool hard: if hard, we will ignore any errors trying to shut
                          down
        """
        if self.service_handles is not None:
            try:
                self.service_handles.shutdown()
            except Exception:
                if not hard:
                    raise

            self.service_handles = None

    def shutdown(self, hard=False):
        """
        Shut down both clients and services.

        :param bool hard: if hard, we will ignore any errors trying to shut
                          down
        """
        self.shutdown_clients(hard=hard)
        self.shutdown_services(hard=hard)

    def scatter_work(self, work, max_thread_count=None, ctx=None):
        """
        Partition the provided iterable of work into roughly even-sized
        portions and send them to each of the remote services. ``ctx`` will be
        copied to each node independently. The user must handle the logic of
        retrieving results and stitching them together. See :func:`gather_ctx`,
        :func:`gather_resp`.

        :param iterable work: iterable of work items
        :param int max_thread_count: override of how many threads each remote
                                     executor should have
        :param object ctx: make sure it is picklable
        :return: a dict mapping (hostname/ip, port) -> worker_id
        """
        out_queues = []
        clients = self.clients
        service_count = len(clients)
        worker_ids = [None] * service_count
        keys = self.clients.keys()
        for _ in range(service_count):
            out_queues.append([])

        # NOTE: do we want to track worker_ids of all the work we started?
        # Meaning: inside our instance?
        client_idx = 0
        work_item_counts = {}
        for work_item in work:
            if self._give_up:
                break

            current_q = out_queues[client_idx]
            current_q.append(encode.encode(work_item))

            current_key = keys[client_idx]
            if len(current_q) == self.WORK_QUANTUM_SIZE:

                if worker_ids[client_idx] is None:
                    logger.debug("Sending %d items to %s", len(current_q),
                            str(current_key))
                    worker_id = clients[current_key].start_work(current_q,
                            max_thread_count=max_thread_count, ctx=ctx)
                    worker_ids[client_idx] = worker_id
                else:
                    logger.debug("Sending %d items to %s", len(current_q),
                            current_key)
                    worker_id = worker_ids[client_idx]
                    clients[current_key].add_work(worker_id, current_q)

                out_queues[client_idx] = []

            client_idx += 1
            client_idx %= service_count

            if current_key not in work_item_counts:
                work_item_counts[current_key] = 0
            work_item_counts[current_key] += 1

        # Tail dump any work not yet flushed
        for client_idx, current_q in enumerate(out_queues):
            if self._give_up:
                break

            if current_q:
                current_key = keys[client_idx]
                if worker_ids[client_idx] is None:
                    logger.debug("Sending %d items to %s", len(current_q),
                            str(current_key))
                    worker_id = clients[current_key].start_work(current_q,
                            max_thread_count=max_thread_count, ctx=ctx)
                    worker_ids[client_idx] = worker_id
                else:
                    logger.debug("Sending %d items to %s", len(current_q),
                            str(current_key))
                    worker_id = worker_ids[client_idx]
                    clients[current_key].add_work(worker_id, current_q)

        worker_ids_out = {}
        for client_idx, current_key in enumerate(keys):
            worker_ids_out[current_key] = worker_ids[client_idx]

        central_logger.log_status("Started %s work items",
                str(work_item_counts))
        return worker_ids_out

    def start_all_on(self, work_item, shared_ctx=None):
        """
        Start a single item of work running on all remote services.

        :param callable work: a single work item
        :param object shared_ctx: make sure it is picklable. Will be shared
                                  across all threads on a given service.
        :return: a dict mapping (hostname/ip, port) -> worker_id
        """
        worker_ids_out = {}
        for ip, client in self.clients.iteritems():
            worker_id = client.start_workers_on(work_item, ctx=shared_ctx)
            worker_ids_out[ip] = worker_id

            if self._give_up:
                break
        return worker_ids_out

    def start_remote_logging(self, ip, port):
        """
        Start logging on all remote services.

        :param str ip: hostname or ip of local machine, where a log server is
                       running
        :param int port: port number of local log server
        """
        for client in self.clients.values():
            client.start_remote_logging(ip, port)

    def run(self, work, ctx=None):
        """
        Scatter work to all remote services, wait for it to finish executing,
        then return gathered responses.

        :param iterable work: iterable of work items
        :param object ctx: make sure it is picklable
        :return: a list of responses, whose ordering is not particularly
                 important.
        """
        worker_ids = self.scatter_work(work, ctx=ctx)
        self.join()
        return self.gather_all_resp(worker_ids)

    def gather_ctx(self, connection, worker_id):
        """
        Gather ``ctx`` from a remote service for a given worker. A running
        :class:`Walk` is free to mutate its ``ctx``, and sometimes that is what
        really constitutes the "response" or "output" of a quantum of work.

        :param tuple connection: str hostname/ip of the remote service, int
                                 port number
        :param int worker_id: id of remote worker, as returned when starting
                              the work (see :func:`scatter_work`).
        """
        ctx = rpyc.utils.classic.obtain(self.clients[connection].gather_ctx(
                worker_id))
        return ctx

    def gather_all_ctxs(self, worker_ids):
        """
        Gather ctxs from all the given workers.

        :param dict worker_ids: a mapping (hostname/ip, port)->worker_id
        :return: a list of ctxs in no particular order.
        """
        # Simple list of ctxs; no order implied.
        ctxs = []
        for con, worker_id in worker_ids.iteritems():
            if isinstance(worker_id, int):
                wids = [worker_id,]
            else:
                wids = worker_id
            if wids is not None:
                for wid in wids:
                    ctx = self.gather_ctx(con, wid)
                    ctxs.append(ctx)

        return ctxs

    def gather_resp(self, connection, worker_id):
        """
        Gather responses from all the given workers.

        :param tuple connection: str hostname/ip of the remote service, int
                                 port number
        :param int worker_id: id of remote worker, as returned when starting
                              the work (see :func:`scatter_work`).
        :return: a response object, as passed to the ``Walk`` that ran on the
                 remote side. See
                 :func:`CoordinatorService.exposed_start_workers_on`
        """
        resp = rpyc.utils.classic.obtain(
                self.clients[connection].gather_resp(worker_id))
        return resp

    def gather_all_resp(self, worker_ids):
        """
        Gather resp from all the given workers.

        :param dict worker_ids: a mapping (hostname/ip, port)->worker_id
        :return: a list of respones in no particular order.
        """
        # Simple list of responses; the user can attach e.g. IPs, hostnames,
        # args, whatever they want to contextualize a response.
        responses = []
        for con, worker_id in worker_ids.iteritems():
            if worker_id is not None:
                resp = self.gather_resp(con, worker_id)
                responses.append(resp)

        return responses

    def join(self, hard=False):
        """
        Wait for all workers on all remote services to complete.
        We can have a finer-grained function later if it is helpful.

        :param bool hard: ignore errors if True
        :raises EOFError, ReferenceError, RuntimeError: if we have an issue
                                                        while joinging (e.g.
                                                        the remote side died)
        """
        for client in self.clients.values():
            try:
                client.join_all_workers()
            except (EOFError, ReferenceError):
                if not hard:
                    raise

def start_remote_services(service_class):
    """
    Simplest method to start some services remotely.

    :param class service_class: a decendent of :class:`CoordinatorService`
    :raises RuntimeError: on various issues with service start up
    :return: A ServiceGroup wrapping the started services
    """
    service_qualname = utils.get_class_qualname(service_class)
    return ServiceGroup(service_qualname)
