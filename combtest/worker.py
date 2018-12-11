"""
This module provides a set of mechanisms for dispatching "work" across a
set of worker nodes. "Work" can mean "execute sets of arbitrary Python
code". Roughly, this system is made of 3 pieces:
 * A basic thread pool implementation. There is a multiprocessing.ThreadPool
   implementation in Python already, but I want the control and flexibility of
   having my own. This and its decendents are the "workers" that actually e.g.
   execute test cases or run fuzz+stress load (see ThreadPool below). Thread
   counts can be chosen by the client, by default, whatever.
 * An rpyc-based coordinator that deserializes work sent to it and sends it
   to a worker/executor of some sort, which will typically be a ThreadPool
   (see CoordinatorService below). Typically there will be one of these per
   node running in its own process, or at least one of a given "type" per
   node. The only constraint is that it needs to have a 1:1 ip/port
   mapping. Typically there will be one central coordinator (ServiceGroup)
   paired with one CoordinatorService/node.
 * A central coordinator running in a single process that is passed work and
   sends it to all paired remote coordinators for execution (ServiceGroup).

Work is sent in some quantum defined by the client code and passed from
ServiceGroup->CoordinatorService->ThreadPool. Responses can be sent back.
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
import multiprocessing
import rpyc
import signal
import sys
import threading
import time
import traceback


import combtest.utils as utils
import combtest.central_logger as central_logger
from combtest.central_logger import logger


# We are going to force requests and responses to be pickled. The reason is
# that netref presents a potential performance landmine, and since we allow the
# user to send arbitrary stuff and do arbitrary things with it, that is not
# something we can control. Instead, lets force them to send things in pickled
# form, which will cut the netrefs and do an actual copy.
rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True


def ensure_service_handle_libs():
    """
    Only import ssh_session on request, since some users may not use it, and it
    brings with it linkage to a metric ton of libs via paramiko.
    Same with isi_config; we only need this for specific stuff below.
    """
    global ssh
    import ssh_session as ssh


# This is an arbitrary target. The idea is that we can't really use our
# resources with just cpu_count threads, since we may spend a ton of time
# sleeping on I/O. User can tweak.
DEFAULT_MAX_THREAD_COUNT = multiprocessing.cpu_count() * 3
DEFAULT_PORT = 6187


class ThreadPool(object):
    """
    A set of threads doing work from a queue asynchronously to the caller. The
    caller can add additional work after the work has started. Child classes
    can add callbacks for e.g. when work starts/finishes, how to handle errors,
    etc.
    """
    # TODO? Move this to py-forkjoin. Probably should.
    def __init__(self, max_thread_count=None, work=None, **kwargs):
        """
        Args:
            max_thread_count - max number of threads to have running at a time.
            work - pop-able iterable of work items. Work items are callable
                   objects that take 0 arguments unless a ctx is supplied, in
                   which case they take 1 argument.
        """
        self._thread_count = max_thread_count or DEFAULT_MAX_THREAD_COUNT
        self._work_queue = work or []
        self._run_ctx = None
        self._running_threads = []
        self.should_stop = False

        self._working = 0
        self._rlock = threading.RLock()

    def add_work(self, work):
        """
        Add work for this pool. Will not kick threads into action if all
        prior work has finished. See kick_workers.
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
        Called when threads are finished up, as indicated by a call to join().
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
        Called if a work_item errors out (via Exception).
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
                logger.debug("Work item took %0.2fs", time.time() - start_time)
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
        This gets all threads running the next work item. Useful for e.g.
        parallel ageing of the same file using the same recipe.
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
    ThreadPool or some decendent class.

    worker_type - a ThreadPool class that workers should be instantiated from
    """
    # User can override
    worker_type = ThreadPool

    # Convenience override point for a base class
    default_max_thread_count = DEFAULT_MAX_THREAD_COUNT

    def on_connect(self):
        """
        rpyc.Service is designed for this to be the de-facto __init__ function.
        So we are doing our initialization here.
        """
        # Maps integer id->worker
        self._workers = {}
        # Maps same integer id->run ctx
        self._ctxs = {}
        self._next_worker_id = 0
        self._rlock = threading.RLock()
        # Maps same integer id->resp
        self._resps = {}

    def _get_next_worker_id(self):
        """
        Get a serial id for the worker. The remote user can use this to refer
        back to the worker.
        """
        with self._rlock:
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
        with self._rlock:
            try:
                return self._workers[worker_id]
            except IndexError:
                raise IndexError("No known worker with id %d" % worker_id)

    def _set_worker_by_id(self, worker_id, worker):
        with self._rlock:
            self._workers[worker_id] = worker

    def _del_worker_by_id(self, worker_id):
        with self._rlock:
            del self._workers[worker_id]

    def work_repack(self, work, ctx=None, resp=None):
        """
        Callback for each individual work item that lets the child class do any
        custom deserialization work. There is no one-size-fits all way of
        packaging work, so we let the user customize that process. If
        depickling the work already did the job, this base class implementation
        is just a no-op.
        Returns: the actual work that will be passed to the worker/ThreadPool.
        """
        return work

    def exposed_start_work(self, work, max_thread_count=None, ctx=None):
        """
        Called by remote side to start some work running asynchronously. The
        work and ctx are pickled and pulled through to this side in whole,
        rather than accessing via netrefs.
        Returns: a worker_id to refer back to this work later for e.g. stopping
                 it or getting responses.
        """
        if max_thread_count is None:
            max_thread_count = self.default_max_thread_count

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

        worker = self.worker_type(max_thread_count=max_thread_count,
                work=packed_work, static_ctx=ctx, resp=resp)
        self._set_worker_by_id(worker_id, worker)
        self._ctxs[worker_id] = ctx
        worker.start(ctx=ctx)

        central_logger.log_status("Started worker with id %d and type %s",
                worker_id, str(type(worker)))

        return worker_id

    def exposed_add_work(self, worker_id, work):
        """
        Add work to an existing worker. The original ctx will be used, since it
        is a "static" ctx. If the user wants work to be executed with a
        different ctx, they start a new worker up via start_work.
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
        the new worker will run that single item. Used for e.g. putting the
        same load on the filesystem from multiple threads on every node.
        Returns: worker_id
        """
        if max_thread_count is None:
            max_thread_count = self.default_max_thread_count
        worker_id = self._get_next_worker_id()
        resp = {}
        self._resps[worker_id] = resp
        work = rpyc.utils.classic.obtain(work)
        ctx = rpyc.utils.classic.obtain(ctx)

        materialized = rpyc.utils.classic.obtain(work)
        packed_work = self.work_repack(materialized, ctx=ctx, resp=resp)
        worker = self.worker_type(max_thread_count=max_thread_count,
                work=[packed_work], static_ctx=ctx, resp=resp)
        self._set_worker_by_id(worker_id, worker)
        self._ctxs[worker_id] = ctx
        worker.start_all_on_next(ctx=ctx)

        logger.debug("Started worker with id %d and type %s", worker_id,
                str(type(worker)))

        return worker_id

    def exposed_run(self, work, max_thread_count=None, ctx=None):
        """
        Run work synchronously. Return the worker_id so that responses/ctxs can
        be reclaimed.
        """
        if max_thread_count is None:
            max_thread_count = self.default_max_thread_count
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

        worker = self.worker_type(max_thread_count=max_thread_count,
                work=packed_work, static_ctx=ctx, resp=resp)
        self._set_worker_by_id(worker_id, worker)
        self._ctxs[worker_id] = ctx

        logger.debug("Running worker with id %d and type %s", worker_id,
                str(type(worker)))

        worker.run(ctx=ctx)
        self._del_worker_by_id(worker_id)
        return worker_id

    def exposed_start_remote_logging(self, ip, port, **kwargs):
        central_logger.add_socket_handler(ip, port)

    def exposed_signal_stop(self, worker_id):
        """
        Signal that all workers should stop. May not be immediate since this is
        intended to be a "clean" shutdown where workers can finish their
        current work item.
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
        """
        self._join_worker(worker_id)

    def exposed_join_all_workers(self):
        """
        Like join_workers, but tosses all outstanding workers.
        """
        with self._rlock:
            worker_ids = [worker_id for worker_id in self._workers.keys()]

        for worker_id in worker_ids:
            self._join_worker(worker_id)

    def exposed_gather_ctx(self, worker_id):
        """
        Return the ctx associated with the given worker.
        """
        return self._ctxs[worker_id]

    def exposed_gather_resp(self, worker_id):
        """
        Return the resp associated with the given worker.
        """
        return self._resps[worker_id]

    def exposed_clean_worker(self, worker_id):
        """
        Toss all state related to a worker. Implies a join first.
        Gives the user full control over when we reap memory, since they may
        still want to retrieve/pickle results. It isn't enough to wait until
        after they gather response, since a.) they may not have "obtained" the
        response value yet, and b.) they may just be gathering intermediate
        results.
        """
        self._join_worker(worker_id)
        del self._resps[worker_id]
        del self._ctxs[worker_id]



# Called from e.g. an ssh session/array using python -c or some such
def start_service(service_class, port=DEFAULT_PORT):
    """
    Start an rpyc service given by provided class. Port can be overridden.
    Returns: a handle to the resulting ThreadedServer.
    """
    from rpyc.utils.server import ThreadPoolServer
    t = ThreadPoolServer(service_class, port=port)
    t.start()
    return t

def start_service_by_name(service_name, port=DEFAULT_PORT):
    """
    Start the service which can be found at the given service_name. The
    service_name is the class's "__qualname__", though that is a python 3+
    concept. For this 2.* implementation we implement the same sort of logic as
    the qualname. Hence if a module a.b.c is imported and has a class D defined
    in it, then D's __qualname__ is a.b.c.D.
    XXX: pay attention to the logic here; it is similar to pickle, or at least
    older versions of pickle: the class must be top level on a module. As
    mentioned in the ServiceGroup.__init__ docstring: lets revisit this iff
    it is needed.
    """
    service_class = utils.get_class_from_qualname(service_name)
    start_service(service_class, port=port)


class ServiceGroup(object):

    """
    Handles to running service instances across the nodes, via SSH, and rpyc-
    based clients connected to those.
    XXX: for now we spawn connections to all nodes. b/w the SSH connections and
    client connections we may run into scalability issues on giant clusters.
    There are obvious optimizations we can do here, such as lazy connecting,
    but it would require some extra hooks. Let's do those changes later when/if
    we prove there is a problem.
    """
    INSTANCES = set()

    # Max time we spend waiting for a service spawn to succeed. We use this
    # since e.g. starting services does not mean the service is immediately
    # available, due to e.g. waiting for imports to happen. Hence the clients
    # need to retry for a bit until the services come up. This timeout is
    # measured in seconds.
    SPAWN_TIMEOUT = 30

    # The maximum chunk of work we will send in a single RPC call. This is to
    # keep the request size sane. If the user gives us an iterable that is
    # generating a massive amount of work on-the-fly we may not be able to
    # instantiate it all and hold it in memory. So we send batches that are
    # this size at maximum.
    WORK_QUANTUM_SIZE = 1000

    def __init__(self, service_name, ips, port=DEFAULT_PORT,
            spawn_services=True, spawn_clients=True):
        """
        Args:
            service_name: path+name of rpyc.Service decendent. Fully qualified
                          pythonic import path, called __qualname__ in Python3.
                          This should give us enough info to import it on the
                          remote side. I'm sure there are edge cases this
                          doesn't allow; upgrade on request.
            ips: should be the "external" interfaces of nodes until we update
                 the logic below to allow anything else.
        """
        ensure_service_handle_libs()

        self._give_up = False
        ServiceGroup.INSTANCES.add(self)

        # (defensive copy, since 'ips' may be mutable)
        self.ips = copy.copy(ips)

        self.service_name = service_name
        self.port = port

        # Will be ssh_session.SSHArray once we spawn
        self.service_handles = None
        # Will be a dict mapping ip->rpyc.connect instances
        self._clients = None

        if spawn_services:
            self.spawn_services()

        # Services and clients are independent since e.g. something else may
        # have started the services already, and we are just connecting.
        if spawn_clients:
            self.spawn_clients()

    @classmethod
    def give_up(cls):
        for instance in cls.INSTANCES:
            instance._give_up = True

    def spawn_services(self):
        """
        Spawn remote services via ssh_session. We keep the SSH handles open. If
        this presents a scalability problem in the future, we will want
        something like 'screen' that allows us to disconnect/reconnect without
        the shell going down.
        """
        assert not self.service_handles, "ERROR: services already running; " \
                "shut them down before spawning again"

        # First bring the services up on the remote side
        self.service_handles = ssh.SSHArray(self.ips)
        logger.debug("Spawned SSH handles for services")
        cmd = ("python -c 'import %s; %s.start_service_by_name(\"%s\", %d)' > "
              "/dev/null" % (ServiceGroup.__module__,
                             ServiceGroup.__module__,
                             self.service_name,
                             self.port))
        logger.debug("Attempting to start services: %s", cmd)
        self.service_handles.start_cmd(cmd)

        logger.debug("Signaled services should start")

    def spawn_clients(self):
        """
        Spawn rpyc clients to the remote services. Assumes remote services are
        up, or will be within SPAWN_TIMEOUT.
        """
        # Retry until all services are up, up to X seconds or whatever. This is
        # just heuristic, so we can be a little sloppy about accounting here.
        self._clients = {}
        for ip in self.ips:
            start_time = time.time()
            while (time.time() - start_time) < self.SPAWN_TIMEOUT and \
                    not self._give_up:
                try:
                    client = rpyc.connect(ip, port=self.port)
                    self._clients[ip] = client
                    break
                except:
                    time.sleep(0.5)

        assert all([ip in self.clients for ip in self.ips]), "ERROR: not " \
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
        Returns a mapping ip->client "root" (see rpyc connect docs)
        """
        out = {}
        if self._clients is not None:
            for ip, client in self._clients.iteritems():
                out[ip] = client.root
        return out

    def shutdown_clients(self, hard=False):
        if not hard:
            assert self.clients, "ERROR: must spawn clients before shutting " \
                    "them down"

        if self._clients:
            for client in self._clients.values():
                client.close()
        self._clients = None

    def shutdown_services(self, hard=False):
        if self.service_handles is not None:
            try:
                self.service_handles.shutdown()
            except Exception:
                if not hard:
                    raise

            self.service_handles = None

    def shutdown(self, hard=False):
        self.shutdown_clients(hard=hard)
        self.shutdown_services(hard=hard)

    def scatter_work(self, work, max_thread_count=None, ctx=None):
        """
        Partition the provided iterable of work into roughly even-sized
        portions and send them to each of the remote services. ctx will be
        copied to each node independently. The user must handle the logic of
        retrieving results and stitching them together. See gather_ctx,
        gather_resp.
        Args:
            work needs to be iterable
        """
        out_queues = []
        clients = self.clients
        ip_count = len(self.ips)
        worker_ids = [None] * ip_count
        for _ in range(ip_count):
            out_queues.append([])

        # NOTE: do we want to track worker_ids of all the work we started?
        # Meaning: inside our instance?
        ip_idx = 0
        work_item_counts = {}
        for work_item in work:
            if self._give_up:
                break

            current_q = out_queues[ip_idx]
            current_q.append(work_item)

            current_ip = self.ips[ip_idx]
            if len(current_q) == self.WORK_QUANTUM_SIZE:

                if worker_ids[ip_idx] is None:
                    logger.debug("Sending %d items to %s", len(current_q),
                            current_ip)
                    worker_id = clients[current_ip].start_work(current_q,
                            max_thread_count=max_thread_count, ctx=ctx)
                    worker_ids[ip_idx] = worker_id
                else:
                    logger.debug("Sending %d items to %s", len(current_q),
                            current_ip)
                    worker_id = worker_ids[ip_idx]
                    clients[current_ip].add_work(worker_id, current_q)

                out_queues[ip_idx] = []

            ip_idx += 1
            ip_idx %= ip_count

            if current_ip not in work_item_counts:
                work_item_counts[current_ip] = 0
            work_item_counts[current_ip] += 1

        # Tail dump any work not yet flushed
        for ip_idx, current_q in enumerate(out_queues):
            if self._give_up:
                break

            if current_q:
                current_ip = self.ips[ip_idx]
                if worker_ids[ip_idx] is None:
                    logger.debug("Sending %d items to %s", len(current_q),
                            current_ip)
                    worker_id = clients[current_ip].start_work(current_q,
                            max_thread_count=max_thread_count, ctx=ctx)
                    worker_ids[ip_idx] = worker_id
                else:
                    logger.debug("Sending %d items to %s", len(current_q),
                            current_ip)
                    worker_id = worker_ids[ip_idx]
                    clients[current_ip].add_work(worker_id, current_q)

        worker_ids_out = {}
        for ip_idx, ip in enumerate(self.ips):
            worker_ids_out[ip] = worker_ids[ip_idx]

        central_logger.log_status("Started %s work items",
                str(work_item_counts))
        return worker_ids_out

    def start_all_on(self, work_item, shared_ctx=None):
        """
        Proxy to exposed_start_all_on
        """
        worker_ids_out = {}
        for ip, client in self.clients.iteritems():
            worker_id = client.start_workers_on(work_item, ctx=shared_ctx)
            worker_ids_out[ip] = worker_id

            if self._give_up:
                break
        return worker_ids_out

    def start_remote_logging(self, ip, port):
        for client in self.clients.values():
            client.start_remote_logging(ip, port)

    def run(self, work, ctx=None):
        """
        Proxy to exposed_run, scattering the work in roughly even-sized chunks
        to all remote services.  Returns a list of responses, whose ordering is
        not particularly important.
        """
        worker_ids = self.scatter_work(work, ctx=ctx)
        self.join()
        return self.gather_all_resp(worker_ids)

    def gather_ctx(self, ip, worker_id):
        """
        Proxy to exposed_gather_ctx for the service at the given ip. Will copy
        the ctx back via pickle.
        """
        ctx = rpyc.utils.classic.obtain(self.clients[ip].gather_ctx(worker_id))
        return ctx

    def gather_resp(self, ip, worker_id):
        """
        Proxy to exposed_gather_resp for the service at the given ip. Will copy
        the resp back via pickle.
        """
        resp = rpyc.utils.classic.obtain(
                self.clients[ip].gather_resp(worker_id))
        return resp

    def gather_all_resp(self, worker_ids):
        """
        Gather resp from all the given workers.
        Args:
            worker_ids: a mapping ip->worker_id
        Returns:
            A list of responses in no particular order.
        """
        # Simple list of responses; the user can attach e.g. IPs, hostnames,
        # args, whatever they want to contextualize a response.
        responses = []
        for ip, worker_id in worker_ids.iteritems():
            if worker_id is not None:
                resp = self.gather_resp(ip, worker_id)
                responses.append(resp)

        return responses

    def join(self, hard=False):
        """
        Join *all* workers for *all* clients. We can have a finer-grained
        function later if it is helpful.
        """
        for client in self.clients.values():
            try:
                client.join_all_workers()
            except (EOFError, ReferenceError):
                if not hard:
                    raise

def start_remote_services(service_class):
    service_qualname = utils.get_class_qualname(service_class)
    return ServiceGroup(service_qualname)
