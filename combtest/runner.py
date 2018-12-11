"""
runner provides a set of tools for running Walks. This includes running Walks
in stages using e.g. SyncPoints, and ways to run Walks in parallel across a
cluster.
"""
from __future__ import print_function

import copy
import json
import os
import shutil
import sys
import threading
import time
import traceback

import isi.ph.localdata as localdata

import isilon.accountant.config as ac_config
import isilon.fuzzsome.trace as trace
import isilon.stream_director.spooled_file

import remote_test_tools as rtt

import filesystem.layout.file_configurator as file_configurator
import filesystem.layout.test_file as tf
import filesystem.layout.worker as worker

from combtest.action import SyncPoint
import combtest.central_logger as central_logger
from combtest.central_logger import logger
import combtest.utils as walk_utils
import combtest.walk as walk
from combtest.walk import WalkEncoder, CancelWalk, WalkFailedError




DEFAULT_LOGGER_PORT = 6188

# Kinda weird looking, I know. Wrapping the value in a list keeps us from
# having to use 'global' later. We set the value to the fqp to a log file if we
# choose to start doing verbose output.
VERBOSE_OUTPUT_SETTING = [False,]
OP_TRACE_SETTING = [False,]

SUBDIR_NAME = "walk_logs"


#### WalkRunners
class WalkRunner(object):
    """
    All threads share one of these. It is a queue of walks, with each thread
    having its own unique idx. The func that is called to execute the walks is
    provided as a qualname by ctx['func'].
    """
    def __init__(self):
        self._walks = []
        self._current_idx = 0
        self._rlock = threading.RLock()

    def add_work(self, work):
        self._walks.append(work)

    def _get_idx(self):
        with self._rlock:
            new_id = self._current_idx
            self._current_idx += 1
        return new_id

    def __call__(self, ctx=None):
        my_idx = self._get_idx()
        func = utils.get_class_from_qualname(ctx['func'])
        return func(self._walks, my_idx)


#### CoordintorServices
class WalkExecutorService(worker.CoordinatorService):
    """
    Simple rpyc service for running a bunch of walks exactly once. Many threads
    will work them in parallel so we don't have to wait forever.
    """
    RUNNER_TYPE = WalkRunnerProvidedFile

    def __init__(self, *args, **kwargs):
        self._next_walker_id = 0
        super(WalkExecutorService, self).__init__(*args, **kwargs)

    @property
    def runner_func(self):
        return None

    def work_repack(self, work, ctx=None, resp=None):
        if 'runner' not in ctx:
            if 'runner_opts' in ctx:
                runner_opts = copy.copy(ctx['runner_opts'])
                runner_opts['func'] = self.runner_func
                ctx['runner'] = self.RUNNER_TYPE(**runner_opts)
            else:
                ctx['runner'] = self.RUNNER_TYPE(func=self.runner_func)

        wr = ctx['runner']

        current_walk = walk.Walk.from_json(work)
        wr.add_work(current_walk)

        return wr

class CountingWalkRunner(object):
    PANIC = False

    def __init__(self, reporting_interval=10000):
        self.count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.reporting_interval = reporting_interval
        self.rlock = threading.RLock()

    def _count(self):
        with self.rlock:
            self.count += 1
            if (self.count % self.reporting_interval) == 0:
                central_logger.log_status("Started running %d walks" %
                        self.count)

    def _count_error(self):
        with self.rlock:
            self.error_count += 1

    def _count_canceled(self):
        with self.rlock:
            self.cancel_count += 1

    def _run_walk(self, walk_to_run, files, verify=True):
        """
        Used in lieu of the ctx['func'] referenced above, to keep things simple for
        the user. We simply pop work as long as there are some left and execute
        them on all the provided test files.
        """
        files_to_remove = []
        for current_file in files:
            logger.debug("Test file: %s; walk: %s", current_file.path,
                    str(repr(walk_to_run)))

            if self.PANIC:
                sys.exit(1)

            if not current_file.op_log_path:
                trace_file = isilon.stream_director.spooled_file.SpooledFile('.',
                        max_size=1024*1024)
                trace_handle = trace.JsonTracer(trace_file)

                current_file.set_log_ops(True, trace_file=trace_handle)

            ctx = walk_utils.CtxTypeFile(current_file)

            canceled = walk_to_run.execute(ctx)
            if canceled:
                self._count_canceled()
                continue

            self._count()

            if verify:
                try:
                    current_file.verify_nonsparse_logical_all()
                except tf.DataIntegrityError as e:
                    new_msg = str(e) + "\nWalk was: %s\nFile was: %s" % \
                            (repr(walk_to_run), current_file.path)
                    actual = ""
                    try:
                        actual = str(localdata.get_pattern_from_file(
                                current_file.path))
                    except ValueError as q:
                        self.PANIC = True
                        new_msg += str(e)
                        raise ValueError(new_msg)

                    if len(actual) > 200:
                        actual = actual[:200]
                    new_msg += "\nActual data on headsnap is: %s" % actual
                    new_msg += "\nOps were: %s" % trace_file.get_string()
                    logger.exception(tf.DataIntegrityError(new_msg))

                    # We remove files from rotation that have some suspected
                    # corruption, in order to preserve state.
                    files_to_remove.append(current_file)

                    self._count_error()

        for current_file in files_to_remove:
            del files[files.index(current_file)]

    def run_walk(self, *args, **kwargs):
        try:
            self._run_walk(*args, **kwargs)
        except CancelWalk:
            self._count_cancel()
        except Exception:
            self._count_error()
            raise

class WalkThreadPool(worker.ThreadPool):
    def on_error_item(self, work_item, ctx, exc, tb):
        logger.exception(exc)

class SimpleWalkExecutorService(worker.CoordinatorService):
    """
    Simplest rpyc-based services for running walks across the cluster: the user
    only needs to provide a list of walks to their ServiceGroup. The rest is
    handled.

    File creation options can be provided via ctx['runner_opts']. See the
    kwargs for WalkRunnerProvidedFailure.__init__ for hints. Example:
        ctx['runner_opts'] = {'prot_pols': ('+1:0', '5x')}
    """
    RUNNER_TYPE = WalkRunnerProvidedFile
    worker_type = WalkThreadPool

    @property
    def runner_func(self):
        if not hasattr(self, 'walk_counter'):
            self.walk_counter = CountingWalkRunner()
        return self.walk_counter.run_walk

    def work_repack(self, work, ctx=None, resp=None):
        if 'runner' not in ctx:
            if 'runner_opts' in ctx:
                runner_opts = copy.copy(ctx['runner_opts'])
                runner_opts['func'] = self.runner_func
                ctx['runner'] = self.RUNNER_TYPE(**runner_opts)
            else:
                ctx['runner'] = self.RUNNER_TYPE(func=self.runner_func)

        wr = ctx['runner']
        current_walk = walk.Walk.from_json(work)
        call = wr.prep_work_call(current_walk)
        return call

    def exposed_start_remote_logging(self, ip, port, log_dir, test_path,
            verbose=False):
        """
        Args:
            ip, port - where we connect our logging socket handler
            log_dir - where we output our logs
            test_path - path on /ifs where the test data will be written
        """
        if verbose and VERBOSE_OUTPUT_SETTING[0] is False:
            central_logger.set_level(central_logger.DEBUG)

            my_ip = rtt.get_my_hostname_and_IP()
            log_fname = str(my_ip) + "." + str(time.time()) + ".log"
            test_file_path = os.path.join(log_dir, log_fname)
            central_logger.add_file_handler(test_file_path)
            central_logger.add_socket_handler(ip, port)

            VERBOSE_OUTPUT_SETTING[0] = test_file_path

        if OP_TRACE_SETTING[0] is False:
            central_logger.add_op_trace(log_dir,
                    walk_utils.WalkOpTracer,
                    test_path)

            OP_TRACE_SETTING[0] = logger.op_trace.fname

        return VERBOSE_OUTPUT_SETTING[0], OP_TRACE_SETTING[0]

    def exposed_gather_resp(self, worker_id):
        resp = self._resps[worker_id]

        if hasattr(self, 'walk_counter'):
            # This should work in all normalish cases. The exception is when a
            # user calls start_work and passes in no work I think.
            resp['count'] = self.walk_counter.count
            resp['error_count'] = self.walk_counter.error_count
            resp['cancel_count'] = self.walk_counter.cancel_count
        else:
            resp['count'] = 0
            resp['error_count'] = 0
            resp['cancel_count'] = 0

        return resp

    def exposed_provide_logs(self, copy_path):
        if VERBOSE_OUTPUT_SETTING[0] is not False:
            try:
                if VERBOSE_OUTPUT_SETTING[0]:
                    shutil.copy(VERBOSE_OUTPUT_SETTING[0], copy_path)
                if OP_TRACE_SETTING[0]:
                    shutil.copy(OP_TRACE_SETTING[0], copy_path)
            except shutil.Error:
                # (probably the same file)
                pass
            return VERBOSE_OUTPUT_SETTING[0], OP_TRACE_SETTING[0]
        return

class WalkRunningUniqueFileExecutorService(SimpleWalkExecutorService):
    RUNNER_TYPE = WalkRunnerUniqueProvidedFile


#### Walk runner funcs; the client should probably just call these.
def run_walks(walk_options, verbose=False, port_logger=DEFAULT_LOGGER_PORT,
        runner_class=SimpleWalkExecutorService, test_path=None,
        **file_config_kwargs):

    # This allows us to receive logs from the remote side that are any log
    # level, but it doesn't actually force the remote side to log at the DEBUG
    # level. See the verbose argument for how we switch that.
    central_logger.set_level(central_logger.DEBUG)

    # Set up remote logging w/local printing
    my_ip = rtt.get_my_hostname_and_IP()
    central_logger.start_recv_remote_logs(my_ip, port_logger)

    # Bring up services across the cluster which can execute Walks in parallel.
    # See worker.py docs on the wiki for details about how this works.
    service_qualname = utils.get_class_qualname(runner_class)
    central_logger.log_status("Bringing up services")
    sg = worker.ServiceGroup(service_qualname)
    sg.start_remote_logging(my_ip, port_logger)
    central_logger.log_status("Services are up")

    sc = walk.StateCombinator(*walk_options)

    try:
        central_logger.log_status("Scattering work")
        start_time = time.time()

        ctx = {}
        if file_config_kwargs:
            ctx['runner_opts'] = copy.copy(file_config_kwargs)
            if test_path:
                ctx['runner_opts']['test_path'] = test_path
        elif test_path:
            ctx['runner_opts'] = {'test_path': test_path}

        if verbose:
            ctx['verbose'] = True

        # Scatter walks to be run across service instances, to be run in parallel.
        # This call is asynchronous; we wait for it to finish with the call to
        # join() below.
        worker_ids = sg.scatter_work(sc.as_json(), ctx=ctx)

        central_logger.log_status("Work sent; waiting for it to finish")
        sg.join()

        central_logger.log_status("Work finished; gathering responses")
        resps = sg.gather_all_resp(worker_ids)
        total_count = 0
        error_count = 0
        for resp in resps:
            if not resp or not 'count' in resp:
                raise RuntimeError("ERROR: did not receive all responses: %s" %
                        str(resps))
            total_count += resp['count']
            error_count += resp['error_count']

        elapsed = time.time() - start_time
    finally:
        central_logger.stop_recv_remote_logs()

    central_logger.log_status("Ran %d walks; %d errors; in %0.2fs",
            total_count, error_count, elapsed)

    return total_count, error_count, elapsed

def run_walks_with_unique_files(walk_options, verbose=False,
        port_logger=DEFAULT_LOGGER_PORT, test_path=None, **file_config_kwargs):
    return run_walks(walk_options, verbose=verbose, port_logger=port_logger,
            test_path=test_path,
            runner_class=WalkRunningUniqueFileExecutorService,
            **file_config_kwargs)


#### Multistage walk running
class CountingWalkRunFunc(CountingWalkRunner):
    def _run_walk(self, walk_to_run, dynamic_ctxs, verify=True):
        for dynamic_ctx in dynamic_ctxs:
            # The first case is 'OMG let's die right now' and the second is
            # 'this file appears corrupted'.
            if self.PANIC or dynamic_ctx['panic']:
                sys.exit(1)

            if dynamic_ctx['cancel']:
                return

            # Reopen file it it is closed
            if dynamic_ctx.test_file is None:
                new_test_file= tf.TestFile(dynamic_ctx['path'],
                        reset_on_open=False, skip_layout=True)
                dynamic_ctx.test_file = new_test_file
            elif dynamic_ctx.test_file.closed:
                dynamic_ctx.test_file.reopen(close=False, trunc=False,
                        relink=True)

            start_time = time.time()
            try:
                canceled = walk_to_run.execute(dynamic_ctx)
                logger.debug("Walk execution took %0.2fs", time.time() -
                        start_time)

                if canceled:
                    dynamic_ctx['cancel'] = True
                    self._count_canceled()
                    continue

                self._count()

                if verify:
                    verify_start_time = time.time()
                    try:
                        dynamic_ctx.test_file.verify_nonsparse_logical_all()
                    except tf.DataIntegrityError as e:
                        new_msg = str(e) + "\nWalk was: %s\nFile was: %s" % \
                                (repr(dynamic_ctx.walk_to_run),
                                 dynamic_ctx.test_file.path)
                        actual = ""
                        try:
                            actual = str(localdata.get_pattern_from_file(
                                    dynamic_ctx.test_file.path))
                        except ValueError as q:
                            self.PANIC = True
                            new_msg += str(q)
                            raise ValueError(new_msg)

                        if len(actual) > 200:
                            actual = actual[:200]
                        new_msg += "\nActual data on headsnap is: %s" % actual
                        det = tf.DataIntegrityError(new_msg)
                        logger.exception(det)

                        # We remove files from rotation that have some
                        # suspected corruption, in order to preserve state.
                        dynamic_ctx['panic'] = True

                        self._count_error()
                        raise det
                logger.debug("Verification took %0.2fs", time.time() -
                        verify_start_time)
            finally:
                # On a multistage walk we will reopen above on the subsequent
                # call ^^.
                dynamic_ctx.test_file.finalize()


class WalkRunnerReclaimable(object):
    """
    Similar to WalkRunnerProvidedFile, but walks are sent with an id that
    allows them to reclaim context from a prior walk. Typically this is used
    for walks to be executed in multiple stages: the second stage is sent with
    an id matching the first.
    When we say "reclaim context" we are referring to the dynamic_ctx.
    Unfortunately we may not be able to keep all the test files open b/w
    stages, so we should just keep track of the path and close the actual file
    handle between stages.
    """
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

    def __init__(self, func, test_path=utils.DEFAULT_TEST_PATH,
            **kwargs):
        self._test_path = test_path
        self._configuration_kwargs = kwargs

        self.my_ip = rtt.get_my_hostname_and_IP()
        self._base_dir = os.path.join(self._test_path, self.my_ip)

        # Maps walk id->[dynamic_ctx1, dynamic_ctx2, ...]
        # The reason we have more than one is that we can run a given walk on
        # multiple files.
        self._dynamic_ctxs = {}
        self._func = func

    def get_walks_ctx(self, walk_id, branch_id, ctx):
        if walk_id not in self._dynamic_ctxs:
            base_dir = ctx['test_path']
            if branch_id:
                base_dir = utils.branch_id_to_dir(branch_id,
                        base_dir=base_dir)
            dir_name = os.path.join(self._base_dir, base_dir)
            dir_name = os.path.join(dir_name, str(walk_id))
            dir_name = os.path.join(dir_name, str(time.time()))

            fc = file_configurator.FileConfigurator(dir_name,
                    **self._configuration_kwargs)
            configs = []
            ctxs = []
            for f, config in fc:
                configs.append(config)
                ctxs.append(walk_utils.CtxTypeFile(f))

            # Now we have created the files, but we close the fds at the end of
            # each walk, so we need the path to re-establish our fd.
            for idx, cur_ctx in enumerate(ctxs):
                cur_ctx['path'] = cur_ctx.test_file.path
                # Marked True if we want to stop touching this guy due to e.g.
                # suspected corruption.
                cur_ctx['panic'] = False
                # Marked True for the same, but only for CancelWalk
                cur_ctx['cancel'] = False
                cur_ctx['file_config'] = configs[idx]

            self._dynamic_ctxs[walk_id] = ctxs

        return self._dynamic_ctxs[walk_id]

    def update_walks_ctx(self, walk_id, update_ctx):
        for dynamic_ctx in self._dynamic_ctxs[walk_id]:
            dynamic_ctx.update(update_ctx)

    def _call(self, walk_id, branch_id, walk_to_run,
            sync_point=None, ctx=None):
        try:
            #import resource
            #print(resource.getrusage(resource.RUSAGE_SELF)[2],
            #        resource.getrusage(resource.RUSAGE_SELF)[2] > 350*1000)
            #if resource.getrusage(resource.RUSAGE_SELF)[2] > (350*1000):
            #    print(resource.getrusage(resource.RUSAGE_SELF)[2])
            #    import pdb
            #    pdb.set_trace()
            #    import filesystem.layout.gc_dump as gc_dump
            #    gc_dump.dump()
            #    sys.exit(1)
            ctxs = self.get_walks_ctx(walk_id, branch_id, ctx=ctx)
            if any([ctx['panic'] for ctx in ctxs]):
                # Skip running later stages of the walk if earlier ones failed.
                pass
            else:
                # They all point to the same physical walk, so update the first
                # only.
                if sync_point is not None:
                    sync_point = walk.action_cached_from_json(sync_point)[0]
                for cur_ctx in ctxs:
                    cur_ctx.update_walk(walk_to_run, sync_point=sync_point)

                if ctx and 'func' in ctx:
                    func = utils.get_class_from_qualname(ctx['func'])
                else:
                    func = self._func

                if ctx and 'verify' in ctx:
                    verify = ctx['verify']
                else:
                    verify = True

                start_time = time.time()
                for cur_ctx in ctxs:
                    logger.trace_op(test_file=cur_ctx.test_file,
                                    walk=cur_ctx.walk,
                                    walk_id=walk_id,
                                    file_config=cur_ctx['file_config'])

                return_val = func(walk_to_run, ctxs, verify=verify)

            #import resource
            #print(resource.getrusage(resource.RUSAGE_SELF)[2],
            #        resource.getrusage(resource.RUSAGE_SELF)[2] > 350*1000)
            #if resource.getrusage(resource.RUSAGE_SELF)[2] > (350*1000):
            #    print(resource.getrusage(resource.RUSAGE_SELF)[2])
            #    import pdb
            #    pdb.set_trace()
            #    import filesystem.layout.gc_dump as gc_dump
            #    gc_dump.dump()
            #    sys.exit(1)
            logger.debug("walk_id %d execution time: %0.2fs",
                    walk_id,
                    time.time() - start_time)
            return return_val
        # TODO: remove
        except MemoryError:
            import pdb; pdb.set_trace()
            #gc_dump.dump()
            raise
        except Exception:
            # TODO
            raise

    def prep_work_call(self, work):
        walk_id, branch_id, walk_to_run, sync_point = work
        def run(ctx=None):
            self._call(walk_id, branch_id, walk_to_run, ctx=ctx,
                    sync_point=sync_point)
        return run

class MultistageWalkRunningService(SimpleWalkExecutorService):
    # The 'runner' reclaims state associated with the walk and wraps up all the
    # args/kwargs for the runner func, then calls the runner func.
    RUNNER_TYPE = WalkRunnerReclaimable

    # The runner_func actually calls the Walk's execute method
    @property
    def runner_func(self):
        if not hasattr(self, 'walk_counter'):
            self.walk_counter = CountingWalkRunFunc()
        return self.walk_counter.run_walk

    def work_repack(self, work, ctx=None, resp=None):
        if 'runner' not in ctx:
            if 'runner_opts' in ctx:
                runner_opts = copy.copy(ctx['runner_opts'])
                runner_opts['func'] = self.runner_func
                ctx['runner'] = self.RUNNER_TYPE.get_instance(**runner_opts)
            else:
                ctx['runner'] = self.RUNNER_TYPE.get_instance(
                        func=self.runner_func)

        wr = ctx['runner']
        walk_id, branch_id, jsonified_walk = json.loads(work)
        current_walk = walk.Walk.from_json(jsonified_walk)
        call = wr.prep_work_call((walk_id, branch_id, current_walk,
                ctx['sync_point']))
        return call

    def exposed_update_remote_contexts(self, worker_id, walk_ids, **kwargs):
        ctx = self.exposed_gather_ctx(worker_id)
        runner = ctx['runner']

        for walk_id in walk_ids:
            runner.update_walks_ctx(walk_id, kwargs)

class ContinuingWalkServiceGroup(worker.ServiceGroup):
    WORK_QUANTUM_SIZE = 10000

    def __init__(self, *args, **kwargs):
        super(ContinuingWalkServiceGroup, self).__init__(*args, **kwargs)
        self.id_map = {}

    def start_all_on(self, work_item, shared_ctx=None):
        raise RuntimeError("You're using this wrong; use scatter_work")

    def run(self, work, ctx=None):
        raise RuntimeError("You're using this wrong; use scatter_work")

    def _flush_queue(self, ip, client, queue, max_thread_count=None, ctx=None,
            sync_point=None):
        logger.debug("Sending %d items to %s", len(queue), ip)

        ctx = copy.copy(ctx)
        if sync_point is not None:
            sync_walk = walk.Walk()
            sync_walk.append(sync_point)
            sync_point = json.dumps(sync_walk, cls=WalkEncoder)
        ctx['sync_point'] = sync_point

        worker_id = client.start_work(queue, max_thread_count=max_thread_count,
                ctx=ctx)
        del queue[:]
        return worker_id

    def scatter_work(self, epoch, id_map=None, max_thread_count=None,
            ctx=None):
        """
        Args:
            epoch - iterable of (walk_id, branch_id, Walk)
            id_map - optional map walk_id->ip of service currently holding that
                     walk's ctx.
        Returns:
            id_map - updated id_map
        """
        # Holds queued work for each IP
        # Maps ip->work queue
        # Flushed when quantum size is hit, and at tail flush
        out_queues = {}

        total_count = 0

        clients = self.clients

        id_map = id_map or self.id_map
        ip_idx = 0
        ip_count = len(self.ips)

        # Maps ip->worker ids
        worker_ids = {}
        for ip in self.ips:
            worker_ids[ip] = []

        # NOTE: 'walk_to_run' is in-fact (walk_id, branch_id, Walk)
        for walk_to_run in epoch:
            walk_id, branch_id, actual_walk = walk_to_run

            if walk_id in id_map:
                target_ip = id_map[walk_id]
            else:
                # Assign the next ip
                target_ip = self.ips[ip_idx]
                ip_idx += 1
                ip_idx %= ip_count
                id_map[walk_id] = target_ip

            if target_ip not in out_queues:
                out_queues[target_ip] = []
            current_queue = out_queues[target_ip]

            jsonified = json.dumps((walk_id, branch_id, actual_walk.as_json()))
            current_queue.append(jsonified)
            if len(current_queue) == self.WORK_QUANTUM_SIZE:
                total_count += len(current_queue)
                worker_id = self._flush_queue(target_ip, clients[target_ip],
                        current_queue, max_thread_count=max_thread_count,
                        ctx=ctx, sync_point=epoch.sync_point)
                worker_ids[target_ip].append(worker_id)

        for ip, queue in out_queues.iteritems():
            client = clients[ip]
            total_count += len(queue)
            worker_id = self._flush_queue(ip, client, queue,
                    max_thread_count=max_thread_count,
                    ctx=ctx, sync_point=epoch.sync_point)
            worker_ids[ip].append(worker_id)

        self.id_map = id_map
        return id_map, total_count, worker_ids

    def update_remote_contexts(self, ip, worker_ids, walk_ids, **kwargs):
        # NOTE: this makes heavy reliance on the singleton being used on the
        # other side.
        worker_id = worker_ids[ip][0]
        self.clients[ip].update_remote_contexts(worker_id, walk_ids, **kwargs)

    def gather_all_resp(self, worker_ids):
        total_count = 0
        error_count = 0
        for ip, ids in worker_ids.iteritems():
            for worker_id in ids:
                resp = self.gather_resp(ip, worker_id)
                if not resp or not 'count' in resp:
                    raise RuntimeError("ERROR: did not receive whole "
                            "response for %s, id %d: %s" %
                            (ip, worker_id, str(resp)))
                total_count += resp['count']
                error_count += resp['error_count']
        return total_count, error_count

    def start_remote_logging(self, ip, port, log_dir, test_path,
            verbose=False):
        logs = {}
        for cur_ip, client in self.clients.iteritems():
            client_logs = client.start_remote_logging(ip, port, log_dir,
                    test_path, verbose=verbose)
            logs[cur_ip] = client_logs
        return logs

    def provide_logs(self, log_dir):
        logs = {}
        for ip, client in self.clients.iteritems():
            client_logs = client.provide_logs(log_dir)
            logs[ip] = client_logs
        return logs


def run_multistage_walks(walk_order, test_path=utils.DEFAULT_TEST_PATH,
        verbose=False,
        port_logger=DEFAULT_LOGGER_PORT,
        runner_class=MultistageWalkRunningService,
        max_thread_count=None,
        log_dir=None,
        **file_config_kwargs):

    if log_dir is None:
        log_dir = ac_config.get_log_dir()
        if log_dir is None or log_dir == ".":
            log_dir = "/var/crash"
    log_dir = os.path.join(log_dir, SUBDIR_NAME)

    my_ip = rtt.get_my_hostname_and_IP()
    rtt.remote_cmd(my_ip, 'isi_for_array "mkdir -p %s"' % log_dir)

    central_logger.log_status("Log files will be at: %s", log_dir)

    # Used to give us some data that connects us back to the remote workers.
    # e.g. where their logs are being stored.
    central_logger.add_op_trace(log_dir, central_logger.OpTracer)
    central_logger.log_status("Log master at: %s", logger.op_trace.fname)

    # This allows us to receive logs from the remote side that are any log
    # level, but it doesn't actually force the remote side to log at the DEBUG
    # level. See the verbose argument for how we switch that.
    central_logger.set_level(central_logger.DEBUG)

    # Set up remote logging w/local printing
    central_logger.start_recv_remote_logs(my_ip, port_logger)

    # Get the test case generator.
    wo = walk.WalkOptions(walk_order)

    # Bring up services across the cluster which can execute Walks in parallel.
    # See worker.py docs on the wiki for details about how this works.
    service_qualname = utils.get_class_qualname(MultistageWalkRunningService)
    central_logger.log_status("Bringing up services")
    sg = ContinuingWalkServiceGroup(service_qualname)

    remote_log_locations = sg.start_remote_logging(my_ip, port_logger, log_dir,
            test_path, verbose=verbose)

    central_logger.log_status("Remote log locations:")
    logger.trace_op(id='master')
    for ip, log_locations in remote_log_locations.iteritems():
        logger.trace_op(ip=ip, logs=log_locations)

    central_logger.log_status("Services are up")

    try:
        central_logger.log_status("Scattering work")
        start_time = time.time()

        ctx = {}
        if file_config_kwargs:
            ctx['runner_opts'] = copy.copy(file_config_kwargs)
        if verbose:
            ctx['verbose'] = True

        ctx['test_path'] = test_path
        ctx['log_dir'] = log_dir

        central_logger.log_status("Test path: %s", test_path)

        master_worker_ids = {}
        for epoch_list in wo:
            central_logger.log_status("Epoch list has %d epochs",
                    len(epoch_list))
            for epoch in epoch_list:
                if epoch.sync_point is not None:
                    for branch_id in epoch.branch_ids:
                        sp_ctx = {"base_dir": test_path,
                                  "branch_id": branch_id,
                                  "service": sg,
                                  "worker_ids": master_worker_ids,
                                  "epoch": epoch
                                 }
                        epoch.sync_point(ctx=sp_ctx)

                _, count, worker_ids = sg.scatter_work(epoch, ctx=ctx,
                        max_thread_count=max_thread_count)
                central_logger.log_status("Epoch of work sent; "
                        "%d work items", count)

                for ip, ids in worker_ids.iteritems():
                    if ip not in master_worker_ids:
                        master_worker_ids[ip] = []
                    master_worker_ids[ip].extend(ids)

            central_logger.log_status("Epochs started; waiting for "
                    "them to finish")
            sg.join()

        central_logger.log_status("Work finished; gathering responses")
        total_count, error_count = sg.gather_all_resp(worker_ids)
        elapsed = time.time() - start_time
        central_logger.log_status("Ran %d walks (%d errors) in %0.2fs" %
                (total_count, error_count, elapsed))

        sg.provide_logs(log_dir)
    finally:
        central_logger.stop_recv_remote_logs()

    return total_count, error_count, elapsed

def replay_multistage_walk(walk_to_run, test_file, step=False, verify=True,
        log_errors=True):
    ctx = walk_utils.CtxTypeFile(test_file)
    print_trace = trace.JsonCallableTracer(log_func=print)
    test_file.set_log_ops(True, trace_file=print_trace)

    try:
        for op in walk_to_run:
            if isinstance(op, SyncPoint):
                op.replay(ctx)
            else:
                op(ctx=ctx)

            if step:
                print(str(type(op)))
                raw_input("Press key to continue...")

        if verify:
            test_file.verify_nonsparse_logical_all()
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
