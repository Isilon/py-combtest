"""
This module provides a set of tools for running Walks. This includes running
Walks in stages using e.g. SerialActions, and ways to run Walks in parallel across
a cluster.
"""
from __future__ import print_function

from collections import namedtuple
import copy
import os
import shutil
import threading
import time
import traceback

import rpyc

from combtest.action import SerialAction
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


Result = namedtuple('Result', 'walk_count error_count segment_count elapsed '
        'states logs')

# NOTE: A WalkRunner is associated 1-to-1 with a worker_id
class WalkRunner(object):
    """
    A WalkRunner simply wraps a ``Walks`` execution method with some tracking
    for reporting stats. The user is free to inherit and add more stats as they
    see fit.
    """
    def __init__(self, reporting_interval=10000):
        self.stats = {'count': 0,
                      'error_count': 0,
                      'cancel_count': 0
                     }
        self.reporting_interval = reporting_interval
        self.rlock = threading.RLock()

        # Simple list of state copies, one for each prepped Walk.
        self._walk_states = None
        self.clean_walk_states()

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

    def get_state(self):
        return copy.copy(self.stats)

    def run_walk(self, walk_to_run, state):
        """
        Called to run an individual ``Walk``.
        """
        self.count_total()
        canceled = walk_to_run.execute(state)
        if canceled:
            raise CancelWalk()

    def prep_work_call(self, walk_to_run, walk_state):
        """
        Called to wrap a ``Walk`` into a callable that accepts a single
        argument: the ``state``.
        """
        self._walk_states.append(walk_state)

        def run(state):
            try:
                self.run_walk(walk_to_run, walk_state)
            except CancelWalk:
                self.count_cancel()
            except Exception as e:
                self.count_error()
                central_logger.log_remote_error(e)
                raise
        return run

    def get_walk_states(self):
        return copy.copy(self._walk_states)

    def clean_walk_states(self):
        states = copy.copy(self._walk_states)
        self._walk_states = []
        return states


class WalkThreadPool(worker.ThreadPool):
    def on_error_item(self, work_item, state, exc, tb):
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

    def work_repack(self, work, shared_state, ctx=None, **repack_kwargs):
        """
        This is called back to repackage each work item sent to the service.
        The call is an opportunity to e.g. do some deserialization, wrap the
        ``Walk`` in a ``WalkRunner``, or anything else the user needs to prep
        the work for execution.

        :param object work: the work to execute; typically this will be e.g. a
                            JSONified ``Walk``.
        :param object shared_state: a ``state`` copied in for executing the
                                    ``Walk``
        :param object ctx: the object returned by repack_ctx before
        """
        runner = ctx['runner']
        current_walk = encode.decode(work)

        # walk_state is a single copy of shared_state. The Runner will store a
        # copy for retrieval later.
        walk_state = copy.copy(shared_state)

        walk_id = self.get_walk_id()
        call = runner.prep_work_call(current_walk, walk_state)
        return call

    def repack_ctx(self, state, **repack_kwargs):
        """
        Called back when a new worker/runner is made to pack up kwargs that we
        pass to the runner's initializer.
        """
        # A single Runner is shared across all Walks in a given worker. If
        # we haven't made that Runner yet, attach it now. Not thread safe,
        # but that is not an issue since we are a single thread getting
        # this state for the first time. No other thread knows the
        # worker_id necessary to mess with this state yet.
        runner_kwargs = repack_kwargs.get('runner_kwargs', {})
        runner = self.WALK_RUNNER_TYPE(**runner_kwargs)

        return {'runner': runner}

    def exposed_start_work(self, work, state, max_thread_count=None,
                           repack_kwargs=None):
        worker_id = super(WalkExecutorService, self).exposed_start_work(work,
                                           state,
                                           max_thread_count=max_thread_count,
                                           repack_kwargs=repack_kwargs,
                                           )

        self._runners[worker_id] = self._ctxs[worker_id]['runner']

        return worker_id

    def exposed_start_workers_on(self, work, state, max_thread_count=None,
                                 repack_kwargs=None):
        worker_id = super(WalkExecutorService, self).exposed_start_workers_on(
                work,
                state,
                max_thread_count=max_thread_count,
                repack_kwargs=repack_kwargs
                )

        self._runners[worker_id] = self._ctxs[worker_id]['runner']

        return worker_id

    def exposed_run(self, work, max_thread_count=None, state=None,
                    repack_kwargs=None):
        worker_id = super(WalkExecutorService, self).exposed_run(work,
                                       max_thread_count=max_thread_count,
                                       state=state,
                                       repack_kwargs=repack_kwargs
                                      )

        self._runners[worker_id] = self._ctxs[worker_id]['runner']

        return worker_id

    def exposed_start_remote_logging(self, ip, port, log_dir=None,
                                     log_namespace=None, verbose=0,
                                     **kwargs):
        """
        Args:
            ip, port - where we connect our logging socket handler
            log_dir - where we output our logs
        """
        my_ip = utils.get_my_IP()
        if verbose == 2:
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
            central_logger.set_level(central_logger.DEBUG)
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

    def exposed_gather_runner_state(self, worker_id):
        resp = {
                'count': 0,
                'error_count': 0,
                'cancel_count': 0,
               }
        try:
            state = self._runners[worker_id].get_state()
            resp.update(state)
        except KeyError:
            # These cases can happen when:
            # * if the user tried to start some work and it threw an exception
            # * they tried to start an empty work list
            # * they never tried to start any work
            # * the worker_id is foobar
            pass

        return resp

    def exposed_gather_state(self, worker_id):
        try:
            runner = self._runners[worker_id]
            walk_states = runner.get_walk_states()
            return walk_states
        except KeyError:
            # These cases can happen when:
            # * if the user tried to start some work and it threw an exception
            # * they tried to start an empty work list
            # * they never tried to start any work
            # * the worker_id is foobar
            pass

        return

    def exposed_clean_worker(self, worker_id):
        super(WalkExecutorService, self).exposed_clean_worker(worker_id)
        runner = self._runners[worker_id]
        del self._runners[worker_id]
        runner.clean_walk_states()

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


#### Multistage walk running. Most everything should use this for convenience.
class MultistageWalkRunner(WalkRunner):
    """
    Similar to ``WalkRunner``, but walks are sent with an id that allows them
    to reclaim state from a prior ``Walk``. Typically this is used for Walks
    to be executed in multiple stages: the second stage is sent with an id
    matching the first.

    Note:
        When we say "reclaim state" we are referring to the ``state``.
    """
    # This is O(N) in memory, where N is the number of ``Walks`` this runner
    # is responsible for. That could present a scalability problem if too many
    # ``Walks`` are run in one shot. Not much we can do unless we start
    # serializing states and stashing them on disk? That is a big change,
    # though. Let's run with this for now.

    def __init__(self, *args, **kwargs):
        super(MultistageWalkRunner, self).__init__(*args, **kwargs)
        # Maps walk_id->dict of extra walk info (e.g. whether it has been canceled)
        self._state_supplement = {}

    def clean_walk_states(self):
        states = copy.copy(self._walk_states)
        self._walk_states = {}
        return states

    # PORT/WRAP - add self.PANIC
    def run_walk(self, walk_id, branch_id, walk_to_run, state,
            sync_point=None):
        walk_id, branch_id, cancel, state = self._get_walks_state(walk_id,
                branch_id, state=state)

        if cancel:
            return

        if sync_point is not None:
            self._set_supplemental_state(walk_id, sync_point=sync_point)

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

        canceled = walk_to_run.execute(state)
        ## Left intentionally for future debugging
        #logger.debug("Walk execution took %0.2fs", time.time() -
        #        start_time)

        if canceled:
            state['cancel'] = True
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

    @property
    def walk_count(self):
        return len(self._walk_states)

    def _set_supplemental_state(self, walk_id, **kwargs):
        self._state_supplement[walk_id].update(**kwargs)

    def _get_walks_state(self, walk_id, branch_id, state):
        # PORT/WRAP
        if walk_id not in self._walk_states:
            supplement = {'walk_id': walk_id,
                          'branch_id': branch_id,
                          'cancel': False,
                          'sync_point': None,
                         }
            self._walk_states[walk_id] = copy.copy(state)
            self._state_supplement[walk_id] = supplement

        supplement = self._state_supplement[walk_id]
        state_out = self._walk_states[walk_id]
        return (supplement['walk_id'], supplement['branch_id'],
                supplement['cancel'], state_out)

    # Refactor artifact
    @property
    def states(self):
        return self.get_states()

    def get_full_states(self, full=False):
        states_out = list()
        for walk_id, state in self.get_walk_states().items():
            if full:
                sup_state = self._state_supplement[walk_id]
                state_out = (state, sup_state)
            else:
                state_out = state
            states_out.append(state_out)
        return states_out

    def update_walks_state(self, walk_id, state_update):
        # PORT/WRAP
        try:
            self._walk_states[walk_id].update(state_update)
        except AttributeError:
            raise AttributeError("To update a state, the state must have "
                                 "an update method (e.g. by virtue of being "
                                 "a dict)")

    def prep_work_call(self, work):
        walk_id, branch_id, walk_to_run, sync_point = work

        if sync_point is not None:
            sync_point = encode.decode(sync_point)

        def run(state):
            try:
                self.run_walk(walk_id, branch_id, walk_to_run, state,
                              sync_point=sync_point)
            except CancelWalk:
                self.count_cancel()
            except Exception as e:
                self.count_error()
                try:
                    self._state_supplement[walk_id]['cancel'] = True
                except KeyError:
                    self._state_supplement[walk_id] = {'cancel': True}
                central_logger.log_remote_error(e)
                raise
        return run

class MultistageWalkRunningService(WalkExecutorService):
    """
    A ``WalkExecutorService`` for executing ``Walks`` in multiple stages. That
    is: we can execute ``Walks`` in parts/slices. A ``walk_id`` is used to
    refer to the ``Walks`` uniquely and is the same across all slices.
    """
    # The 'runner' reclaims state associated with the walk and wraps up all the
    # args/kwargs for the runner func, then calls the runner func.
    WALK_RUNNER_TYPE = MultistageWalkRunner

    def work_repack(self, work, shared_state, ctx=None, **repack_kwargs):
        runner = ctx['runner']
        current_walk = encode.decode(work)

        walk_id, branch_id, current_walk = encode.decode(work)
        call = runner.prep_work_call((walk_id, branch_id, current_walk,
                                      repack_kwargs.get('sync_point', None)))
        return call

    def repack_ctx(self, state, **repack_kwargs):
        """
        Called back when a new worker/runner is made to pack up kwargs that we
        pass to the runner's initializer.
        """
        # A single Runner is shared across all Walks in a given worker. If
        # we haven't made that Runner yet, attach it now. Not thread safe,
        # but that is not an issue since we are a single thread getting
        # this state for the first time. No other thread knows the
        # worker_id necessary to mess with this state yet.
        runner_kwargs = repack_kwargs.get('runner_kwargs', {})
        runner = self.WALK_RUNNER_TYPE.get_instance(**runner_kwargs)

        return {'runner': runner}

    def exposed_update_remote_states(self, worker_id, walk_ids, state_update):
        try:
            runner = self._runners[worker_id]

            state_update = rpyc.utils.classic.obtain(state_update)

            for walk_id in walk_ids:
                runner.update_walks_state(walk_id, state_update)
        except KeyError as e:
            raise RuntimeError("Could not update Walk's state; did an "
                               "earlier stage fail?\n%s" % str(e))

    def exposed_gather_state(self, worker_id):
        try:
            runner = self._runners[worker_id]
            states = runner.get_full_states(full=False)
            return encode.encode(states)
        except KeyError:
            pass

        return

    def exposed_gather_full_state(self, worker_id):
        try:
            runner = self._runners[worker_id]
            states = runner.get_full_states(full=False)
            return encode.encode(states)
        except KeyError:
            pass

        return

    def exposed_gather_runner_state(self, worker_id):
        resp = {}
        try:
            resp = super(MultistageWalkRunningService,
                         self).exposed_gather_runner_state(worker_id)

            resp['segment_count'] = resp['count']
            del resp['count']

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
    :class:`combtest.action.SerialAction` during a run.
    """
    WORK_QUANTUM_SIZE = 10000

    def __init__(self, *args, **kwargs):
        super(ContinuingWalkServiceGroup, self).__init__(*args, **kwargs)
        # Maps walk_id->(ip, port) of service where that walk's ctx is
        # currently held. We track this so we can lock the affinity of a
        # walk to a single service - allowing us to actually reclaim ctx.
        self.id_map = {}

    def start_all_on(self, work_item, shared_state):
        raise RuntimeError("You're using this wrong; use scatter_work")

    def run(self, work, state):
        raise RuntimeError("You're using this wrong; use scatter_work")

    def _flush_queue(self, client_key, client, queue, max_thread_count=None,
                     state=None, sync_point=None):
        logger.debug("Sending %d items to %s", len(queue), str(client_key))

        state = copy.copy(state)
        if sync_point is not None:
            sync_point = encode.encode(sync_point)
            repack_kwargs = {'sync_point': sync_point}
        else:
            repack_kwargs = None

        worker_id = client.start_work(queue, state,
                                      max_thread_count=max_thread_count,
                                      repack_kwargs=repack_kwargs)
        del queue[:]
        return worker_id

    def scatter_work(self, epoch, id_map=None, max_thread_count=None,
                     state=None):
        """
        Scatter some ``Walk`` segments out to the remote workers.

        :param iterable epoch: iterable of (walk_id, branch_id, Walk)
        :param dict id_map: optional map walk_id->(ip, port) of service
                            currently holding that Walk's state.
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
                        state=state, sync_point=epoch.sync_point)
                worker_ids[target_key].append(worker_id)

        for target_key, queue in out_queues.items():
            client = clients[target_key]
            total_count += len(queue)
            worker_id = self._flush_queue(target_key, client, queue,
                                          max_thread_count=max_thread_count,
                                          state=state,
                                          sync_point=epoch.sync_point)
            worker_ids[target_key].append(worker_id)

        self.id_map = id_map
        return id_map, total_count, worker_ids

    def update_remote_states(self, ip, worker_ids, walk_ids, state):
        """
        Push an update to the ``states`` on the remote executors. This is
        sometimes useful when a :class:`combtest.action.SerialAction` executes and
        affects the state of some set of ``Walks``.

        :param str ip: hostname or IP where the remote executor can be found
        :param iterable worker_ids: iterable of ints, corresponding to
                                    ``worker_ids`` that can be found on the
                                    remote executor.
        :param object state: something JSONifiable; the remote state will be
                             updated via its update() method. The simplest
                             way to support this is by using a dict as a state.
        """
        # NOTE: this makes heavy reliance on the singleton being used on the
        # other side: all workers are sharing a 'Runner'
        worker_id = worker_ids[ip][0]
        self.clients[ip].update_remote_states(worker_id, walk_ids, state)

    def gather_runner_state(self, connection, worker_id):
        """
        Gather ``state`` from a remote runner for a given worker. This is an
        optional internal state object provided by
        :func:`WalkRunner.get_state`.

        :param tuple connection: str hostname/ip of the remote service, int
                                 port number
        :param int worker_id: id of remote worker, as returned when starting
                              the work (see :func:`scatter_work`).
        """
        state = rpyc.utils.classic.obtain(
                self.clients[connection].gather_runner_state(worker_id))
        return state

    def gather_all_runner_states(self, worker_ids):
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
        for connection, ids in worker_ids.items():
            for worker_id in ids:
                resp = self.gather_runner_state(connection, worker_id)
                if not resp or 'segment_count' not in resp:
                    raise RuntimeError("ERROR: did not receive whole "
                                       "response for %s, id %d: %s" %
                                       (str(connection), worker_id, str(resp)))
                total_segment_count += resp['segment_count']
                total_error_count += resp['error_count']
                total_walk_count += resp['walk_count']
        return total_segment_count, total_error_count, total_walk_count

    def gather_state(self, connection, worker_id, full=False):
        """
        Gather ``state`` from a remote service for a given worker. A running
        :class:`Walk` is free to mutate its ``state``, and sometimes that is
        what really constitutes the "response" or "output" of a quantum of
        work.

        :param tuple connection: str hostname/ip of the remote service, int
                                 port number
        :param int worker_id: id of remote worker, as returned when starting
                              the work (see :func:`scatter_work`).
        """
        if full:
            state = rpyc.utils.classic.obtain(
                    self.clients[connection].gather_full_state(worker_id))
        else:
            state = rpyc.utils.classic.obtain(
                    self.clients[connection].gather_state(worker_id))
        return encode.decode(state)

    def gather_all_states(self, worker_ids, full=False):
        """
        Gather states from all the given workers.

        :param dict worker_ids: a mapping (hostname/ip, port)->worker_id
        :return: a list of states in no particular order.
        """
        # Simple list of states; no order implied.
        states = []
        for con, worker_id in worker_ids.items():
            if isinstance(worker_id, int):
                wids = [worker_id,]
            else:
                wids = worker_id
            if wids is not None:
                for wid in wids:
                    state = self.gather_state(con, wid, full=full)
                    states.append(state)

        return states

    def start_remote_logging(self, ip, port, log_dir=None, log_namespace=None,
                             verbose=1):
        """
        Start logging on all remote services.

        :param str ip: hostname or ip of local machine, where a log server is
                       running
        :param int port: port number of local log server
        :param str log_dir: path to remote log directory the service should
                            use
        :param str log_namespace: prefix for log files
        :param int verbose: 0-2 verbosity setting. As 2 an additional verbose
                            level log will be produced.
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
def run_tests(walk_order,
              state=None,
              verbose=1,
              logger_port=None,
              runner_class=MultistageWalkRunningService,
              service_group_class=ContinuingWalkServiceGroup,
              service_infos=None,
              service_handler_class=bootstrap.ServiceHandler_Local,
              max_thread_count=None,
              gather_states=False,
              log_dir=None,
              # PORT/WRAP test_path=utils.DEFAULT_TEST_PATH,
              #**file_config_kwargs,
             ):
    """
    Run a collection of :class:`combtest.walk.Walk`. This should be the main
    way to execute ``Walks`` for most users. This is the only interface that
    supports correct execution of a :class:`combtest.action.SerialAction`.

     You can provide some instance to serve as the state passed around during
     the tests. There are two important details to know about this:
      * The state must be JSON-ifiable, but py-combtest provides a convenience
        pattern to help with that. See :func:`encode`.
      * Shallow copies of the state will be made, via copy.copy(), since each
        test owns its own copy. You may want to e.g. override __copy__ if
        the details of the copy are important to you.

    :param iterable walk_order: An iterable of iterables which produce
                                :class:`combtest.action.Action`. Example: a
                                list of iterables produced by
                                ``MyActionClass.get_option_set()``.
    :param object state: a state to copy and pass to the ``Walks`` when we
                         execute them.
    :param int verbose: 0-2 verbosity setting. At 2 an additional verbose level
                        log will be produced.
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
    :param bool gather_states: if True or 1, gather and return all ``states``
                               from the remote services at the end of the run.
                               Will be returned as a mapping ip->[state, ...].
                               if 2, gather extra info about the run of the
                               walk, such as if it was canceled.
                               else the returned states will be None
    :param int max_thread_count: Max number of ``Walk`` executing threads that
                                 each service will use.
    :param str log_dir: Directory where we will store traces, debug logs, etc.
                        Remote services will also attempt to store logs to
                        the same path.

    :raises RuntimeError: when remote services can't be established and
                          connected to.
    :return: count of walks run, count of walk execution errors, count of walk
             segments run, total elapsed time, remote states if
             ``gather_state == True`` else None, the location of the master
             log file, where applicable.
    """

    if logger_port is None:
        logger_port = config.get_logger_port()


    if verbose == 0:
        central_logger.set_level(central_logger.WARNING)
    elif verbose == 1:
        central_logger.set_level(central_logger.INFO)
    else:
        central_logger.set_level(central_logger.DEBUG)


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

    # Set up remote logging w/local printing
    central_logger.start_recv_remote_logs(my_ip, logger_port)

    sg = None

    try:
        # Get the test case generator.
        wo = walk.WalkOptions(walk_order)

        # Bring up services across the cluster which can execute Walks in parallel.
        # See worker.py docs on the wiki for details about how this works.
        service_qualname = utils.get_class_qualname(runner_class)
        central_logger.log_status("Bringing up services to run some tests")
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

        remote_logs = []
        for logs in remote_log_locations.values():
            remote_logs.extend(logs)
        if any(remote_logs):
            logger.trace_op(id='master')
            for ip, log_locations in remote_log_locations.items():
                logger.trace_op(ip=ip,
                                logs=log_locations)
            master_location = logger.op_trace.fname
        master_log = {
                      'master': master_location,
                      'remote': remote_log_locations
                     }

        logger.info("Services are up")

        logger.info("Scattering work")
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
            logger.info("Epoch list has %d epochs",
                                      len(epoch_list))
            for epoch in epoch_list:
                state_copy = copy.copy(state)
                if epoch.sync_point is not None:
                    for branch_id in epoch.branch_ids:
                        # PORT/WRAP
                        #sp_ctx = {"base_dir": test_path,
                        #          "branch_id": branch_id,
                        #          "service": sg,
                        #          "worker_ids": master_worker_ids,
                        #          "epoch": epoch
                        #         }
                        state_copy = epoch.sync_point(state=state_copy,
                                                      branch_id=branch_id,
                                                      epoch=epoch,
                                                      service=sg,
                                                      worker_ids=master_worker_ids
                                                     )

                _, count, worker_ids = sg.scatter_work(epoch,
                                             state=state_copy,
                                             max_thread_count=max_thread_count)
                logger.info("Epoch of work sent; %d work items", count)

                for connection_info, ids in worker_ids.items():
                    if connection_info not in master_worker_ids:
                        master_worker_ids[connection_info] = []
                    master_worker_ids[connection_info].extend(ids)

            logger.info("Epochs started; waiting for "
                                      "them to finish")
            sg.join()

        logger.info("Work finished; gathering responses")

        segment_count = 0
        error_count = 0
        walk_count = 0
        for connection_info, ids in master_worker_ids.items():
            if len(ids) == 0:
                # No work sent, e.g. because we didn't have many walks
                continue

            # NOTE: taking advantage of singleton
            wid = ids[0]
            wids = {connection_info: [wid,]}
            current_segment_count, current_error_count, current_walk_count = \
                    sg.gather_all_runner_states(wids)
            segment_count += current_segment_count
            error_count += current_error_count
            walk_count += current_walk_count

        elapsed = time.time() - start_time
        central_logger.log_status("Ran %d walks (%d errors) in %0.2fs" %
                                  (walk_count, error_count, elapsed))

        if gather_states is True or gather_states == 1:
            states_out = sg.gather_all_states(worker_ids, full=False)
        elif gather_states == 2:
            states_out = sg.gather_all_states(worker_ids, full=True)
        else:
            states_out = None

        if log_dir is not None:
            sg.provide_logs(log_dir)
    finally:
        central_logger.stop_recv_remote_logs()

        try:
            if sg is not None:
                sg.shutdown(hard=True)
        except Exception:
            pass

    return Result(walk_count, error_count, segment_count, elapsed, states_out,
                  master_log)


# TODO: PORT/WRAP
def replay_walk(walk_to_run, step=False, log_errors=True, state=None):
    """
    Run a single :class:`combtest.walk.Walk`

    :param Walk walk_to_run: self evident
    :param bool step: if True, step Action-by-Action through the Walk; the user
                      hits a key to proceed to the next Action.
    :param bool log_errors: log exceptions to the logger if True
    :param object state: state passed to the Walk for execution.
    """
    try:
        for op in walk_to_run:
            op(state=state)

            if step:
                print(str(type(op)))
                raw_input("Press key to continue...")

        # PORT/WRAP if verify:
        # PORT/WRAP     test_file.verify_nonsparse_logical_all()
    except CancelWalk as e:
        return True
    except Exception as e:
        msg_state = "Walk was: %s\nstate: %s" % (repr(walk_to_run),
                encode.encode(state))

        if log_errors:
            logger.exception(e)
            logger.error(msg_state)

        new_msg = "Walk failed:\n"
        new_msg += traceback.format_exc()
        new_msg += "\n" + msg_state
        wfe = WalkFailedError(new_msg)
        if hasattr(e, 'errno'):
            wfe.errno = e.errno
        raise wfe

    return False
