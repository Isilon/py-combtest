"""
This module provides a set of tools for running Walks. This includes running
Walks in stages using e.g. SyncPoints, and ways to run Walks in parallel across
a cluster.
"""
from __future__ import print_function

import copy
import os
import shutil
import threading
import time
import traceback

from combtest.action import SyncPoint
import combtest.bootstrap as bootstrap
import combtest.central_logger as central_logger
from combtest.central_logger import logger
import combtest.config as config
import combtest.encode as encode
import combtest.utils as utils
import combtest.walk as walk
from combtest.walk import CancelWalk, WalkFailedError, \
                          WalkOpTracer
import combtest.worker as worker



# NOTE: A WalkRunner is associated 1-to-1 with a worker_id
class WalkRunner(object):
    """
    A WalkRunner simply wraps a ``Walks`` execution method with some tracking
    for reporting stats. The user is free to inherit and add more stats as they
    see fit.
    """
    def __init__(self, resp_object, reporting_interval=10000):
        self.stats = {'count': 0,
                      'error_count': 0,
                      'cancel_count': 0
                     }
        self.reporting_interval = reporting_interval
        self.rlock = threading.RLock()

        # This class doesn't use this, but it could be used by a descendent if
        # desired. Here for child classes to use.
        self.resp = resp_object

    def count_total(self):
        """
        Called to count the number of ``Walks``/Walk segments we started
        running.
        """
        with self.rlock:
            self.stats['count'] += 1
            if (self.count % self.reporting_interval) == 0:
                central_logger.log_status("Started running %d walks" %
                                          self.stats['count'])

    def count_error(self):
        """
        Called on a ``Walk`` execution error.
        """
        with self.rlock:
            self.stats['error_count'] += 1

    def count_cancel(self):
        """
        Called when a ``Walk`` is canceled.
        """
        with self.rlock:
            self.stats['cancel_count'] += 1

    @property
    def count(self):
        return self.stats['count']

    @property
    def error_count(self):
        return self.stats['error_count']

    @property
    def cancel_count(self):
        return self.stats['cancel_count']

    def run_walk(self, walk_to_run, ctx=None):
        """
        Called to run an individual ``Walk``.
        """
        self.count_total()
        canceled = walk_to_run.execute(ctx)
        if canceled:
            raise CancelWalk()

    def prep_work_call(self, walk_to_run, walk_ctx):
        """
        Called to wrap a ``Walk`` into a callable that accepts a single
        argument: the ``ctx``.
        """
        def run(ctx=None):
            try:
                self.run_walk(walk_to_run, ctx=walk_ctx)
            except CancelWalk:
                self.count_cancel()
            except Exception as e:
                self.count_error()
                central_logger.log_remote_error(e)
                raise
        return run


class WalkThreadPool(worker.ThreadPool):
    def on_error_item(self, work_item, ctx, exc, tb):
        logger.exception(exc)


class WalkExecutorService(worker.CoordinatorService):
    """
    Simplest rpyc-based services for running walks across the cluster: the user
    only needs to provide a list of ``Walks`` to their corresponding ServiceGroup.
    The rest is handled.

    Note:
        The user should probably not be calling into this directly. They
        should be starting it up via rpyc.
    """
    # TODO: PORT/WRAP
    #: Override the type of ``WalkRunner`` used, here or in a child class
    WALK_RUNNER_TYPE = WalkRunner
    #: Override the type of ``ThreadPool`` used for executing ``Walks``, here
    #: or in a child class
    WORKER_TYPE = WalkThreadPool

    def on_connect(self, conn):
        super(WalkExecutorService, self).on_connect(conn)

        # Maps worker_id->WalkRunner instance
        self._runners = {}

        self._service_port = config.get_service_port()

        self.verbose_file_path = None
        self.trace_file_path = None

        self._next_walk_id = 0

    def get_walk_id(self):
        with self.rlock:
            id_out = self._next_walk_id
            self._next_walk_id += 1
        return id_out

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
        if 'runner' not in ctx:
            if resp is None:
                resp = {}
            resp['resps'] = []

            runner_kwargs = ctx.get('runner_kwargs', {})
            ctx['runner'] = self.WALK_RUNNER_TYPE(resp, **runner_kwargs)

            ctx['walk_ctxs'] = []

        runner = ctx['runner']
        current_walk = encode.decode(work)

        walk_ctx = copy.copy(ctx)
        ctx['walk_ctxs'].append(walk_ctx)

        walk_id = self.get_walk_id()
        call = runner.prep_work_call(current_walk, walk_ctx)
        return call

    def exposed_start_work(self, work, max_thread_count=None, ctx=None):
        worker_id = super(WalkExecutorService, self).exposed_start_work(work,
                                                                        max_thread_count=max_thread_count,
                                                                        ctx=ctx
                                                                       )

        self._runners[worker_id] = self._ctxs[worker_id].get('runner', None)

        return worker_id

    def exposed_start_workers_on(self, work, max_thread_count=None, ctx=None):
        worker_id = super(WalkExecutorService, self).exposed_start_workers_on(
                work,
                max_thread_count=max_thread_count,
                ctx=ctx,
                )

        self._runners[worker_id] = self._ctxs[worker_id].get('runner', None)

        return worker_id

    def exposed_run(self, work, max_thread_count=None, ctx=None):
        worker_id = super(WalkExecutorService, self).exposed_run(work,
                                                                 max_thread_count=max_thread_count,
                                                                 ctx=ctx,
                                                                )

        self._runners[worker_id] = self._ctxs[worker_id].get('runner', None)

        return worker_id

    def exposed_start_remote_logging(self, ip, port, log_dir=None,
                                     log_namespace=None, verbose=False,
                                     **kwargs):
        """
        Args:
            ip, port - where we connect our logging socket handler
            log_dir - where we output our logs
        """
        my_ip = utils.get_my_IP()
        if verbose:
            if self.verbose_file_path is None:
                central_logger.set_level(central_logger.DEBUG)


                if log_dir is not None:
                    log_fname = "%s.%d.%d.log" % (str(my_ip), self._service_port,
                                                  int(time.time()))
                    if log_namespace is not None:
                        log_fname = log_namespace + "." + log_fname
                    verbose_file_path = os.path.join(log_dir, log_fname)
                    self.verbose_file_path = verbose_file_path
                    central_logger.add_file_handler(verbose_file_path)

        if self.trace_file_path is None and log_dir is not None:
            if log_namespace is None:
                log_namespace = log_dir
            log_namespace += "%s.%d.%d.log" % (str(my_ip), self._service_port,
                                               int(time.time()))

            central_logger.add_op_trace(log_dir,
                                        WalkOpTracer,
                                        log_namespace)

            self.trace_file_path = logger.op_trace.fname

        central_logger.add_socket_handler(ip, port)

        return (self.verbose_file_path,
                self.trace_file_path)

    def exposed_gather_resp(self, worker_id):
        resp = self._resps[worker_id]

        resp['count'] = 0
        resp['error_count'] = 0
        resp['cancel_count'] = 0
        try:
            runner = self._runners[worker_id]
            resp.update(runner.stats)
        except KeyError:
            # These cases can happen when:
            # * if the user tried to start some work and it threw an exception
            # * they tried to start an empty work list
            # * they never tried to start any work
            # * the worker_id is foobar
            pass

        return resp

    def exposed_gather_ctx(self, worker_id):
        try:
            ctx = self._ctxs[worker_id]

            # Remove stuff we added inline, like the runner
            ctxs_out = []
            for walk_ctx in ctx['walk_ctxs']:
                ctx_out = {}
                for k, v in walk_ctx.items():
                    if k in ['runner', 'walk_ctxs']:
                        continue
                    ctx_out[k] = v
                ctxs_out.append(ctx_out)

            return ctxs_out
        except KeyError:
            pass

        return

    def exposed_provide_logs(self, copy_path):
        if self.verbose_file_path is not None:
            try:
                shutil.copy(self.verbose_file_path, copy_path)
            except shutil.Error:
                # (probably the same file)
                pass

        if self.trace_file_path is not None:
            try:
                shutil.copy(self.trace_file_path, copy_path)
            except shutil.Error:
                # (probably the same file)
                pass

        return (self.verbose_file_path,
                self.trace_file_path)


#### Walk runner funcs; the client should probably just call these.
def run_walks(walk_options,
              ctx=None,
              verbose=False,
              logger_port=None,
              runner_class=WalkExecutorService,
              service_group_class=worker.ServiceGroup,
              service_infos=None,
              service_handler_class=bootstrap.ServiceHandler_Local,
              gather_ctxs=False,
              max_thread_count=None,
              **runner_kwargs):
    """
    Run a collection of :class:`combtest.walk.Walk`. This is one of the main
    functions that the user should probably be using to run their tests.

    :param iterable walk_options: An iterable of iterables which produce
                                  :class:`combtest.action.Action`. Example: a
                                  list of iterables produced by
                                  ``MyActionClass.get_option_set()``.
    :param object ctx: a state/``ctx`` to pass to copy and pass to the
                       ``Walks`` when we execute them.
    :param bool verbose: produce a verbose level log, and set the log level to
                         DEBUG.
    :param int logger_port: the port number where our local logger should
                            accept data.
    :param combtest.worker.CoordinatorService runner_class: the type of Walk
                                                            execution service
                                                            to use.
    :param combtest.worker.ServiceGroup service_group_class: the type of
                                                             ``ServiceGroup``
                                                             we will use to
                                                             coordinate remote
                                                             executors
    :param iterable service_infos: An iterable of any extra infos we need to
                                   bootstrap the remote services. See
                                   :class:`combtest.bootstrap.ServiceHandleArray`.
    :param combtest.bootstrap.ServiceHandler service_handler_class: Type of
                        ``ServiceHandler`` to use to bootstrap the services.
    :param bool gather_ctxs: if True, gather and return all ``ctxs`` from the
                             remote services at the end of the run. Will be
                             returned as a mapping ip->[ctx, ...]
    :param int max_thread_count: Max number of ``Walk`` executing threads that
                                 each service will use.
    :param runner_kwargs: kwargs to pass to the remote ``WalkRunner``.

    :raises RuntimeError: when remote services can't be established and
                          connected to.
    :return: count of walks run, count of walk execution errors, total elapsed
             time, remote ctxs if ``gather_ctxs == True``, else None
    """

    if logger_port is None:
        logger_port = config.get_logger_port()

    # This allows us to receive logs from the remote side that are any log
    # level, but it doesn't actually force the remote side to log at the DEBUG
    # level. See the verbose argument for how we switch that.
    central_logger.set_level(central_logger.DEBUG)

    # Set up remote logging w/local printing
    my_ip = utils.get_my_IP()

    central_logger.start_recv_remote_logs(my_ip, logger_port)

    sg = None

    try:
        # Bring up services across the cluster which can execute Walks in parallel.
        # See worker.py docs on the wiki for details about how this works.
        service_qualname = utils.get_class_qualname(runner_class)
        central_logger.log_status("Bringing up services")
        sg = service_group_class(service_qualname,
                                 service_infos=service_infos,
                                 service_handler_class=service_handler_class
                                )

        sg.start_remote_logging(my_ip, logger_port)

        central_logger.log_status("Services are up")

        sc = walk.StateCombinator(*walk_options)

        ctxs_out = []

        try:
            central_logger.log_status("Scattering work")
            start_time = time.time()

            ctx = ctx or {}
            ctx['runner_kwargs'] = copy.copy(runner_kwargs)

            if verbose:
                ctx['verbose'] = True

            # Scatter walks to be run across service instances, to be run in parallel.
            # This call is asynchronous; we wait for it to finish with the call to
            # join() below.
            worker_ids = sg.scatter_work(sc,
                                         ctx=ctx,
                                         max_thread_count=max_thread_count
                                        )

            central_logger.log_status("Work sent; waiting for it to finish")
            sg.join()

            central_logger.log_status("Work finished; gathering responses")
            resps = sg.gather_all_resp(worker_ids)
            total_count = 0
            error_count = 0
            for resp in resps:
                if not resp or 'count' not in resp:
                    raise RuntimeError("ERROR: did not receive all responses: %s" %
                                       str(resps))
                total_count += resp['count']
                error_count += resp['error_count']

            elapsed = time.time() - start_time

            if gather_ctxs:
                ctxs_out = sg.gather_all_ctxs(worker_ids)

        finally:
            central_logger.stop_recv_remote_logs()

            try:
                if sg is not None:
                    sg.shutdown(hard=True)
            except Exception:
                pass

        central_logger.log_status("Ran %d walks; %d errors; in %0.2fs",
                                  total_count, error_count, elapsed)

        return total_count, error_count, elapsed, ctxs_out
    finally:
        central_logger.stop_recv_remote_logs()


#### Multistage walk running
class MultistageWalkRunner(WalkRunner):
    """
    Similar to ``WalkRunner``, but walks are sent with an id that allows them
    to reclaim context from a prior ``Walk``. Typically this is used for Walks
    to be executed in multiple stages: the second stage is sent with an id
    matching the first.

    Note:
        When we say "reclaim context" we are referring to the ``dynamic_ctx`` /
        ``ctx``.
    """
    # This is O(N) in memory, where N is the number of ``Walks`` this runner
    # is responsible for. That could present a scalability problem if too many
    # ``Walks`` are run in one shot. Not much we can do unless we start
    # serializing ctxs and stashing them on disk? That is a big change, though.
    # Let's run with this for now.

    # PORT/WRAP - add self.PANIC
    def run_walk(self, walk_id, branch_id, walk_to_run,
                 sync_point=None, ctx=None):
        ctx = self.get_walks_ctx(walk_id, branch_id, ctx=ctx)

        if 'cancel' in ctx and ctx['cancel']:
            return

        if sync_point is not None:
            ctx['sync_point'] = sync_point


        # PORT/WRAP - add self.PANIC
        #logger.trace_op(test_file=cur_ctx.test_file,
        #                walk=cur_ctx.walk,
        #                walk_id=walk_id,
        #                file_config=cur_ctx['file_config'])
        logger.trace_op(walk=walk_to_run,
                        walk_id=walk_id,
                        sync_point=sync_point,
                        branch_id=branch_id)
        #start_time = time.time()

        self.count_total()

        canceled = walk_to_run.execute(ctx)
        ## Left intentionally for future debugging
        #logger.debug("Walk execution took %0.2fs", time.time() -
        #        start_time)

        if canceled:
            ctx['cancel'] = True
            self.count_cancel()


    # Singleton
    INSTANCE = None
    INSTANCE_LOCK = threading.RLock()

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls.INSTANCE is None:
            with cls.INSTANCE_LOCK:
                if cls.INSTANCE is None:
                    cls.INSTANCE = cls(*args, **kwargs)
        return cls.INSTANCE


    def __init__(self, *args, **kwargs):
        super(MultistageWalkRunner, self).__init__(*args, **kwargs)

        # Maps walk id->[dynamic_ctx1, dynamic_ctx2, ...]
        # The reason we have more than one is that we can run a given walk on
        # multiple files.
        self._dynamic_ctxs = {}

    @property
    def walk_count(self):
        return len(self._dynamic_ctxs)

    def get_walks_ctx(self, walk_id, branch_id, ctx):
        # PORT/WRAP
        if walk_id not in self._dynamic_ctxs:
            walk_ctx = copy.copy(ctx)
            walk_ctx['walk_id'] = walk_id
            walk_ctx['branch_id'] = branch_id
            walk_ctx['cancel'] = False

            self._dynamic_ctxs[walk_id] = walk_ctx

        return self._dynamic_ctxs[walk_id]

    @property
    def ctxs(self):
        return copy.copy(self._dynamic_ctxs)

    def update_walks_ctx(self, walk_id, update_ctx):
        # PORT/WRAP
        self._dynamic_ctxs[walk_id].update(update_ctx)

    def prep_work_call(self, work):
        walk_id, branch_id, walk_to_run, sync_point = work

        if sync_point is not None:
            sync_point = encode.decode(sync_point)

        def run(ctx=None):
            try:
                self.run_walk(walk_id, branch_id, walk_to_run, ctx=ctx,
                              sync_point=sync_point)
            except CancelWalk:
                self.count_cancel()
            except Exception as e:
                self.count_error()
                ctx['cancel'] = True
                central_logger.log_remote_error(e)
                raise
        return run

class MultistageWalkRunningService(WalkExecutorService):
    """
    A ``WalkExecutorService`` for executing ``Walks`` in multiple stages. That
    is: we can execute ``Walks`` in parts/slices. A ``walk_id`` is used to
    refer to the ``Walks`` uniquely.
    """
    # The 'runner' reclaims state associated with the walk and wraps up all the
    # args/kwargs for the runner func, then calls the runner func.
    RUNNER_TYPE = MultistageWalkRunner

    def work_repack(self, work, ctx=None, resp=None):
        if 'runner' not in ctx:
            if resp is None:
                resp = {}
            resp['resps'] = []

            if 'runner_opts' in ctx:
                runner_opts = copy.copy(ctx['runner_opts'])
                ctx['runner'] = self.RUNNER_TYPE.get_instance(resp, **runner_opts)
            else:
                ctx['runner'] = self.RUNNER_TYPE.get_instance(resp)

        wr = ctx['runner']
        walk_id, branch_id, current_walk = encode.decode(work)
        call = wr.prep_work_call((walk_id, branch_id, current_walk,
                                  ctx.get('sync_point', None)))
        return call

    def exposed_update_remote_contexts(self, worker_id, walk_ids, **kwargs):
        try:
            ctx = self._ctxs[worker_id]
            runner = ctx['runner']

            for walk_id in walk_ids:
                runner.update_walks_ctx(walk_id, kwargs)
        except KeyError:
            raise RuntimeError("Could not update walk's context; did an "
                               "earlier stage fail?")

    def exposed_gather_ctx(self, worker_id):
        try:
            runner = self._runners[worker_id]
            ctxs_out = {}

            for walk_id, walk_ctx in runner.ctxs.items():
                ctx_out = {}
                for k, v in walk_ctx.items():
                    if k in ['runner', 'walk_ctxs']:
                        continue
                    ctx_out[k] = v
                ctxs_out[walk_id] = ctx_out

            return ctxs_out
        except KeyError:
            pass

        return

    def exposed_gather_resp(self, worker_id):
        resp = super(MultistageWalkRunningService,
                     self).exposed_gather_resp(worker_id)

        resp['segment_count'] = resp['count']
        del resp['count']
        resp['walk_count'] = 0
        try:
            runner = self._runners[worker_id]

            resp['walk_count'] = runner.walk_count
        except KeyError:
            # These cases can happen when:
            # * if the user tried to start some work and it threw an exception
            # * they tried to start an empty work list
            # * they never tried to start any work
            # * the worker_id is foobar
            pass

        return resp

class ContinuingWalkServiceGroup(worker.ServiceGroup):
    """
    A ``ServiceGroup`` designed for running ``Walks`` in stages. We need to be
    able to run ``Walks`` in stages in order to satisfy the semantic of
    :class:`combtest.action.SyncPoint` during a run.
    """
    WORK_QUANTUM_SIZE = 10000

    def __init__(self, *args, **kwargs):
        super(ContinuingWalkServiceGroup, self).__init__(*args, **kwargs)
        # Maps walk_id->(ip, port) of service where that walk's ctx is
        # currently held. We track this so we can lock the affinity of a
        # walk to a single service - allowing us to actually reclaim ctx.
        self.id_map = {}

    def start_all_on(self, work_item, shared_ctx=None):
        raise RuntimeError("You're using this wrong; use scatter_work")

    def run(self, work, ctx=None):
        raise RuntimeError("You're using this wrong; use scatter_work")

    def _flush_queue(self, client_key, client, queue, max_thread_count=None,
                     ctx=None, sync_point=None):
        logger.debug("Sending %d items to %s", len(queue), str(client_key))

        ctx = copy.copy(ctx) or {}
        if sync_point is not None:
            sync_point = encode.encode(sync_point)
        ctx['sync_point'] = sync_point

        worker_id = client.start_work(queue, max_thread_count=max_thread_count,
                                      ctx=ctx)
        del queue[:]
        return worker_id

    def scatter_work(self, epoch, id_map=None, max_thread_count=None,
                     ctx=None):
        """
        Scatter some ``Walk`` segments out to the remote workers.

        :param iterable epoch: iterable of (walk_id, branch_id, Walk)
        :param dict id_map: optional map walk_id->(ip, port) of service
                            currently holding that Walk's ctx.
        :return: an updated ``id_map``
        """
        # Holds queued work for each IP
        # Maps client_key->work queue
        # Flushed when quantum size is hit, and at tail flush
        out_queues = {}

        total_count = 0

        clients = self.clients

        id_map = id_map or self.id_map
        key_idx = 0
        keys = list(self.clients.keys())
        key_count = len(keys)

        # Maps (ip, port)->worker ids
        worker_ids = {}
        for key in keys:
            worker_ids[key] = []

        # NOTE: 'walk_to_run' is in-fact (walk_id, branch_id, Walk)
        for walk_to_run in epoch:
            walk_id, branch_id, actual_walk = walk_to_run

            if walk_id in id_map:
                target_key = id_map[walk_id]
            else:
                # Assign the next client
                target_key = keys[key_idx]
                key_idx += 1
                key_idx %= key_count
                id_map[walk_id] = target_key

            if target_key not in out_queues:
                out_queues[target_key] = []
            current_queue = out_queues[target_key]

            jsonified = encode.encode((walk_id, branch_id, actual_walk))
            current_queue.append(jsonified)
            if len(current_queue) == self.WORK_QUANTUM_SIZE:
                total_count += len(current_queue)
                worker_id = self._flush_queue(target_key, clients[target_key],
                        current_queue, max_thread_count=max_thread_count,
                        ctx=ctx, sync_point=epoch.sync_point)
                worker_ids[target_key].append(worker_id)

        for target_key, queue in out_queues.items():
            client = clients[target_key]
            total_count += len(queue)
            worker_id = self._flush_queue(target_key, client, queue,
                                          max_thread_count=max_thread_count,
                                          ctx=ctx, sync_point=epoch.sync_point)
            worker_ids[target_key].append(worker_id)

        self.id_map = id_map
        return id_map, total_count, worker_ids

    def update_remote_contexts(self, ip, worker_ids, walk_ids, **kwargs):
        """
        Push an update to the ``ctxs`` on the remote executors. This is
        sometimes useful when a :class:`combtest.action.SyncPoint` executes and
        affects some set of ``Walks``.

        :param str ip: hostname or IP where the remote executor can be found
        :param iterable worker_ids: iterable of ints, corresponding to
                                    ``worker_ids`` that can be found on the
                                    remote executor.
        :param kwargs: the keys/values to set on the remote ``ctx``.
        """
        # NOTE: this makes heavy reliance on the singleton being used on the
        # other side: all workers are sharing a 'runner'
        worker_id = worker_ids[ip][0]
        self.clients[ip].update_remote_contexts(worker_id, walk_ids, **kwargs)

    def gather_all_resp(self, worker_ids):
        """
        Gather stats from remote workers. User may want to extend this to
        include any new stats they think of.

        :param dict worker_ids: a mapping hostname/ip->iterable of
                                ``worker_ids`` from which we should
                                gather responses.
        :return: count of Walk segments run, count of Walk execution errors,
                 count of Walks run
        """
        total_segment_count = 0
        total_error_count = 0
        total_walk_count = 0
        for ip, ids in worker_ids.items():
            for worker_id in ids:
                resp = self.gather_resp(ip, worker_id)
                if not resp or 'segment_count' not in resp:
                    raise RuntimeError("ERROR: did not receive whole "
                                       "response for %s, id %d: %s" %
                                       (ip, worker_id, str(resp)))
                total_segment_count += resp['segment_count']
                total_error_count += resp['error_count']
                total_walk_count += resp['walk_count']
        return total_segment_count, total_error_count, total_walk_count

    def start_remote_logging(self, ip, port, log_dir=None, log_namespace=None,
                             verbose=False):
        """
        Start logging on all remote services.

        :param str ip: hostname or ip of local machine, where a log server is
                       running
        :param int port: port number of local log server
        :param str log_dir: path to remote log directory the service should
                            use
        :param str log_namespace: prefix for log files
        :param bool verbose: True to signal debug logging
        :return: a dict mapping remote service hostname/ip->remote log file
                 locations
        """
        logs = {}
        for cur_ip, client in self.clients.items():
            client_logs = client.start_remote_logging(
                    ip,
                    port,
                    log_dir=log_dir,
                    log_namespace=log_namespace,
                    verbose=verbose)
            logs[cur_ip] = client_logs
        return logs

    def provide_logs(self, log_dir):
        logs = {}
        for ip, client in self.clients.items():
            client_logs = client.provide_logs(log_dir)
            logs[ip] = client_logs
        return logs

# TODO: PORT/WRAP
def run_multistage_walks(walk_order,
                         ctx=None,
                         verbose=False,
                         logger_port=None,
                         runner_class=MultistageWalkRunningService,
                         service_group_class=ContinuingWalkServiceGroup,
                         service_infos=None,
                         service_handler_class=bootstrap.ServiceHandler_Local,
                         max_thread_count=None,
                         gather_ctxs=False,
                         log_dir=None,
                         # PORT/WRAP test_path=utils.DEFAULT_TEST_PATH,
                         #**file_config_kwargs,
                        ):
    """
    Run a collection of :class:`combtest.walk.Walk`. This should be the main
    way to execute ``Walks`` for most users. This is the only interface that
    supports correct execution of a :class:`combtest.action.SyncPoint`.

    :param iterable walk_order: An iterable of iterables which produce
                                :class:`combtest.action.Action`. Example: a
                                list of iterables produced by
                                ``MyActionClass.get_option_set()``.
    :param object ctx: a state/``ctx`` to pass to copy and pass to the
                       ``Walks`` when we execute them.
    :param bool verbose: produce a verbose level log, and set the log level to
                         DEBUG.
    :param int logger_port: the port number where our local logger should
                            accept data.
    :param combtest.worker.CoordinatorService runner_class: the type of Walk
                                                            execution service
                                                            to use.
    :param combtest.worker.ServiceGroup service_group_class: the type of
                                                             ``ServiceGroup``
                                                             we will use to
                                                             coordinate remote
                                                             executors
    :param iterable service_infos: An iterable of any extra infos we need to
                                   bootstrap the remote services. See
                                   :class:`combtest.bootstrap.ServiceHandleArray`.
    :param combtest.bootstrap.ServiceHandler service_handler_class: Type of
                        ``ServiceHandler`` to use to bootstrap the services.
    :param bool gather_ctxs: if True, gather and return all ``ctxs`` from the
                             remote services at the end of the run. Will be
                             returned as a mapping ip->[ctx, ...]
    :param int max_thread_count: Max number of ``Walk`` executing threads that
                                 each service will use.
    :param str log_dir: Directory where we will store traces, debug logs, etc.
                        Remote services will also attempt to store logs to
                        the same path.

    :raises RuntimeError: when remote services can't be established and
                          connected to.
    :return: count of walks run, count of walk execution errors, count of walk
             segments run, total elapsed time, remote ctxs if
             ``gather_ctxs == True`` else None, the location of the master
             log file, where applicable.
    """


    if logger_port is None:
        logger_port = config.get_logger_port()

    # PORT/WRAP
    #if log_dir is None:
    #    log_dir = ac_config.get_log_dir()
    #    if log_dir is None or log_dir == ".":
    #        log_dir = "/var/crash"
    #log_dir = os.path.join(log_dir, SUBDIR_NAME)

    my_ip = utils.get_my_IP()
    #rtt.remote_cmd(my_ip, 'isi_for_array "mkdir -p %s"' % log_dir)

    if log_dir is not None:
        central_logger.log_status("Log files will be at: %s", log_dir)

        # Used to give us some data that connects us back to the remote
        # workers. e.g. where their logs are being stored.
        central_logger.add_op_trace(log_dir, central_logger.OpTracer)
        central_logger.log_status("Log master at: %s", logger.op_trace.fname)
        # TODO? Pull files back from remote side via rpyc?

    # This allows us to receive logs from the remote side that are any log
    # level, but it doesn't actually force the remote side to log at the DEBUG
    # level. See the verbose argument for how we switch that.
    central_logger.set_level(central_logger.DEBUG)

    # Set up remote logging w/local printing
    central_logger.start_recv_remote_logs(my_ip, logger_port)
    ctxs_out = []

    sg = None

    ctx = ctx or {}

    try:
        # Get the test case generator.
        wo = walk.WalkOptions(walk_order)

        # Bring up services across the cluster which can execute Walks in parallel.
        # See worker.py docs on the wiki for details about how this works.
        service_qualname = utils.get_class_qualname(runner_class)
        central_logger.log_status("Bringing up services")
        sg = service_group_class(service_qualname,
                                 service_infos=service_infos,
                                 service_handler_class=service_handler_class
                                )

        # PORT/WRAP: pass test_path
        remote_log_locations = sg.start_remote_logging(my_ip,
                                                       logger_port,
                                                       log_dir,
                                                       verbose=verbose)
        #            test_path, verbose=verbose)

        master_location = ""
        if any([remote_log_locations.values()]):
            central_logger.log_status("Remote log locations:")
            logger.trace_op(id='master')
            for ip, log_locations in remote_log_locations.items():
                logger.trace_op(ip=ip, logs=log_locations)
            master_location = logger.op_trace.fname
        master_log = {'master': master_location,
                      'remote': remote_log_locations
                     }

        central_logger.log_status("Services are up")

        central_logger.log_status("Scattering work")
        start_time = time.time()

        # PORT/WRAP
        #ctx = {}
        #if file_config_kwargs:
        #    ctx['runner_opts'] = copy.copy(file_config_kwargs)
        #if verbose:
        #    ctx['verbose'] = True

        #ctx['test_path'] = test_path
        #ctx['log_dir'] = log_dir

        # central_logger.log_status("Test path: %s", test_path)

        master_worker_ids = {}
        for epoch_list in wo:
            central_logger.log_status("Epoch list has %d epochs",
                                      len(epoch_list))
            for epoch in epoch_list:
                ctx_copy = copy.copy(ctx)
                if epoch.sync_point is not None:
                    # This logic should be pulled into SyncPoint. I don't like
                    # that SyncPoint.update_remote_contexts relies on this
                    # being set up right.
                    for branch_id in epoch.branch_ids:
                        # PORT/WRAP
                        #sp_ctx = {"base_dir": test_path,
                        #          "branch_id": branch_id,
                        #          "service": sg,
                        #          "worker_ids": master_worker_ids,
                        #          "epoch": epoch
                        #         }
                        ctx_copy = epoch.sync_point(ctx=ctx_copy,
                                                    branch_id=branch_id,
                                                    epoch=epoch,
                                                    service=sg,
                                                    worker_ids=master_worker_ids
                                                   )

                _, count, worker_ids = sg.scatter_work(epoch, ctx=ctx_copy,
                                                       max_thread_count=max_thread_count)
                central_logger.log_status("Epoch of work sent; "
                                          "%d work items", count)

                for ip, ids in worker_ids.items():
                    if ip not in master_worker_ids:
                        master_worker_ids[ip] = []
                    master_worker_ids[ip].extend(ids)

            central_logger.log_status("Epochs started; waiting for "
                                      "them to finish")
            sg.join()

        central_logger.log_status("Work finished; gathering responses")

        segment_count = 0
        error_count = 0
        walk_count = 0
        for ip, ids in master_worker_ids.items():
            if len(ids) == 0:
                # No work sent, e.g. because we didn't have many walks
                continue

            # NOTE: taking advantage of singleton
            wid = ids[0]
            wids = {ip: [wid,]}
            current_segment_count, current_error_count, current_walk_count = \
                    sg.gather_all_resp(wids)
            segment_count += current_segment_count
            error_count += current_error_count
            walk_count += current_walk_count

        elapsed = time.time() - start_time
        central_logger.log_status("Ran %d walks (%d errors) in %0.2fs" %
                                  (walk_count, error_count, elapsed))

        if gather_ctxs:
            ctxs_out = sg.gather_all_ctxs(worker_ids)

        if log_dir is not None:
            sg.provide_logs(log_dir)
    finally:
        central_logger.stop_recv_remote_logs()

        try:
            if sg is not None:
                sg.shutdown(hard=True)
        except Exception:
            pass

    return (walk_count, error_count, segment_count, elapsed, ctxs_out,
            master_log)


# TODO: PORT/WRAP
def replay_multistage_walk(walk_to_run, step=False, log_errors=True, ctx=None):
    """
    Run a single :class:`combtest.walk.Walk`

    :param Walk walk_to_run: self evident
    :param bool step: if True, step Action-by-Action through the Walk; the user
                      hits a key to proceed to the next Action.
    :param bool log_errors: log exceptions to the logger if True
    :param object ctx: state/``ctx`` passed to the Walk for execution.
    """
    try:
        for op in walk_to_run:
            if isinstance(op, SyncPoint):
                op.replay(ctx)
            else:
                op(ctx=ctx)

            if step:
                print(str(type(op)))
                raw_input("Press key to continue...")

        # PORT/WRAP if verify:
        # PORT/WRAP     test_file.verify_nonsparse_logical_all()
    except CancelWalk as e:
        return True
    except Exception as e:
        msg_ctx = "Walk was: %s\nctx: %s" % (repr(walk_to_run), str(ctx))

        if log_errors:
            logger.exception(e)
            logger.error(msg_ctx)

        new_msg = "Walk failed:\n"
        new_msg += traceback.format_exc()
        new_msg += "\n" + msg_ctx
        wfe = WalkFailedError(new_msg)
        if hasattr(e, 'errno'):
            wfe.errno = e.errno
        raise wfe

    return False
