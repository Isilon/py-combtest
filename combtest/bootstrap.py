"""
We need *some* way to bootstrap the rpyc-based service
("combtest.worker.CoordinatorService") on the remote node. We don't need to
make too many assumptions to do so. Let's provide a general interface for
accomplishing this (ConnectionArray), and a base SSH implementation,
and then let the client code swap it out. combtest.worker uses this interface
to bootstrap the service instances.
"""

import atexit
import copy
import multiprocessing
import os
import sys
import time


import combtest.central_logger as central_logger
import combtest.config as config
import combtest.utils as utils
# TODO: upstream forkjoin as a separate lib
import combtest.forkjoin as forkjoin


# We want to provide a default function for bootstrapping services. Our default
# is in 'combtest.worker', and the user can provide their own. But we don't
# want bootstrap to depend on worker. This will rarely change. So how about we
# hardcode and let the user override as they see fit?  Unfortunately IDEs wont
# see a reference here (I don't think?). What are alternatives?
DEFAULT_SERVICE_RUN_FUNC = 'combtest.worker.start_service_by_name'


class ConnectionInfo(object):
    def __init__(self, ip, port, service_info):
        self.ip = ip
        self.port = port
        self.service_info = service_info

    def __repr__(self):
        return str((self.ip, self.port, self.service_info))

    def __str__(self):
        return repr(self)


# Global tracking of oustanding (not yet shutdown) instances, for cleanup
# purposes.

class ServiceHandler(object):
    INSTANCES = []

    def __init__(self, connection_info):
        self._service_info = connection_info.service_info
        self._service_ip = connection_info.ip
        self._service_port = connection_info.port

        self.INSTANCES.append(self)

        # (client code to implement actual connection spawning in subclass)

    @property
    def service_info(self):
        return copy.copy(self._service_info)

    @property
    def service_port(self):
        return self._service_port

    @property
    def service_ip(self):
        return self._service_ip

    def start_cmd(self, service_class,
            service_run_func=DEFAULT_SERVICE_RUN_FUNC):
        raise NotImplementedError()

    def shutdown(self):
        # (client code to implement actual connection shutdown in subclass)
        self.INSTANCES.remove(self)

    @classmethod
    def shutdown_all(cls):
        instances = copy.copy(cls.INSTANCES)
        for instance in instances:
            instance.shutdown()


class ServiceHandler_Local(ServiceHandler):
    # In seconds
    SHUTDOWN_TIMEOUT = 120

    def __init__(self, connection_info):
        if connection_info.port is None or connection_info.ip is None:
            connection_info.ip = utils.get_my_IP()
            connection_info.port = config.get_service_port()
        super(ServiceHandler_Local, self).__init__(connection_info)

        self._proc = None

    def start_cmd(self,
                  service_class,
                  service_run_func=DEFAULT_SERVICE_RUN_FUNC):

        if isinstance(service_run_func, basestring):
            service_run_func = utils.get_class_from_qualname(service_run_func)

        if not isinstance(service_class, basestring):
            service_qualname = utils.get_class_qualname(service_class)
        else:
            service_qualname = service_class

        proc = multiprocessing.Process(target=service_run_func,
                                       args=(service_qualname,
                                             self.service_port,
                                            )
                                      )
        # NOTE: even daemonized it will still share stdout/err with this proc.
        # That is a good thing imho, since we will see tracebacks. SSH-based
        # services will not enjoy the same advantage by default.`
        proc.daemon = True
        proc.start()

        # TODO: make sure we didn't die quick? But how long should we wait to
        # find out? And what if we die during executing a handler? A
        # ping-response pattern to check aliveness seems crazy overkill for a
        # lib like this, but not hard.
        #
        # If the user set up remote logging, they will probably see the
        # traceback for a death during execution. But they won't see a start up
        # death, since they won't have called in to set up remote logging yet.
        # Up to them to do logging to file? That seems fair for now.

        self._proc = proc

    def shutdown(self):
        if self._proc is None:
            return

        pid = str(self._proc.pid)

        central_logger.logger.debug("Sending shutdown signal to %s" % pid)

        self._proc.terminate()

        start_time = time.time()
        finished = False
        while (time.time() - start_time) < self.SHUTDOWN_TIMEOUT:
            finished = not self._proc.is_alive()
            if finished:
                break

        if not finished:
            raise RuntimeError("ERROR: did not see service die on %s" % pid)

        central_logger.logger.debug("Shutdown complete on %s" % pid)
        self._proc = None
        super(ServiceHandler_Local, self).shutdown()


class ServiceHandleArray(object):
    # To be a subclass of ServiceHandler
    REMOTE_CONNECTION_CLASS = ServiceHandler

    INSTANCES = []

    def __init__(self, connection_infos, spawn=True):
        """
        """
        # Maps repr(connection_info)->Remote for open Remotes
        self.instances = {}

        if any([not isinstance(ci, ConnectionInfo) for ci in connection_infos]):
            raise ValueError("Must provide connection infos as ConnectionInfos")
        self._connection_infos = copy.copy(connection_infos)

        self.INSTANCES.append(self)

        if spawn:
            self.spawn_many(self._connection_infos)

    def attach(self, connection_info, instance):
        key = repr(connection_info)
        assert key not in self.instances, "ERROR: we already have an " \
                "instance for %s" % key
        self.instances[key] = instance

    def dettach(self, key):
        """
        Dettach an instance from this array and return it. This does not close
        the instance; simply removes it from the array.
        """
        assert key in self.instances, "ERROR: no such instance %s" % key
        instance = self.instances[key]
        del self.instances[key]
        return instance

    @property
    def is_alive(self):
        # For now: don't try to check connection states. Let's just check if we
        # have any connections.
        return bool(self.instances)

    def spawn(self, connection_info):
        # We assert here in addition to self.attach to prevent even attempting
        # a connection if we already have one, while still having an assert in
        # self.attach for users that are hitting it directly.
        key = repr(connection_info)
        assert key not in self.instances, "ERROR: we already have an " \
                "instance for %s" % key
        instance = self.REMOTE_CONNECTION_CLASS(connection_info)
        self.attach(connection_info, instance)

    def spawn_many(self, connection_infos=None):
        work = []

        if connection_infos is not None:
            self._connection_infos = copy.copy(connection_infos)

        for connection_info in self._connection_infos:
            work_item = forkjoin.WorkItem(self.spawn,
                                          (connection_info, ),
                                          {}
                                         )
            work.append(work_item)

        results = forkjoin.fork_join(work)
        for idx, result in results.items():
            if isinstance(result, BaseException):
                connection_info = self._connection_infos[idx]
                sys.stderr.write("ERROR spawning (%s): " %
                        repr(connection_info))
                raise result

    def shutdown_single(self, key):
        """
        Shutdown a single instance and stop tracking it.

        :raises KeyError: if 'ip' does not correspond to an instance we are
                          tracking.
        """
        instance = self.dettach(key)
        instance.shutdown()

    def shutdown(self, hard=False):
        """
        Shutdown all instances this array is tracking.
        """
        work = []
        keys = copy.copy(self.instances.keys())
        for key in keys:
            work_item = forkjoin.WorkItem(self.shutdown_single, (key,), {})
            work.append(work_item)

        results = forkjoin.fork_join(work)
        if not hard:
            for idx, result in results.items():
                if isinstance(result, BaseException):
                    key = keys[idx]
                    sys.stderr.write("ERROR shutting down (%s): " % key)
                    raise result

        self.INSTANCES.remove(self)

    def foreach_serial(self, cmd, include=None, exclude=None, expected_exit=0):
        if not self.is_alive:
            raise RuntimeError("ERROR: no live connections")

        # Map connection_info->output
        outputs = {}

        for connection_info, instance in self.instances.iteritems():
            if include and connection_info not in include:
                continue

            if exclude and connection_info in exclude:
                continue

            output, exitcode = instance.send_cmd(cmd)
            if exitcode != expected_exit:
                raise RuntimeError("Got bad exit running on %s: %s: %s" %
                        (connection_info, str(exitcode), str(output)))

            outputs[connection_info] = output

        return outputs

    def foreach(self, cmd, include=None, exclude=None, expected_exit=0,
            timeout=0.5):
        """
        Run a command on all boxes in parallel

        :param cmd: command to run in the shell as a string
        :param include: ips of boxes where we will run the command. Default is
                        to run on all.
        :param exclude: ips of boxes where we will not run the command. Default
                        is not to exclude any.
        :param expected_exit: (optional) assert the exit code is equal to this,
                               or if this is ERRNO_IGNORE, don't assert. Assert
                               for all boxes on which we run the cmd.
        :returns: A dict mapping ip -> output, where output is:
                  (output, exitcode)
        """
        if not self.is_alive:
            raise RuntimeError("ERROR: no live connections")

        def run_async_op(connection_info, cmd):
            result = self.instances[connection_info].cmd(cmd, timeout=timeout)
            return result

        work_items = []
        for connection_info in self.instances:
            if include and connection_info not in include:
                continue

            if exclude and connection_info in exclude:
                continue

            work_item = forkjoin.WorkItem(run_async_op, (connection_info,
                    cmd), {})
            work_items.append(work_item)

        results = forkjoin.fork_join(work_items, suppress_errors=True)
        # Map connection_info->output
        outputs = {}
        for work_item_idx, result in results.iteritems():
            if result and not isinstance(result, BaseException):
                output, exitcode = result
            else:
                output = None
                exitcode = None

            work_item = work_items[work_item_idx]
            connection_info = work_item.args[0]

            if exitcode is None or exitcode != expected_exit:
                raise RuntimeError("Got bad exit running on %s: %s" %
                        (connection_info, str(results)))
            else:
                outputs[connection_info] = output

        return outputs

    def __getattr__(self, attr):
        """
        If this array does not have this attribute, we will look for the
        attribute in the individual Remotes and return a func ptr to a
        forkjoined foreach to call the attribute. I refer to this in some
        places as "dynamic foreach dispatch."
        """
        try:
            return self.__dict__[attr]
        except KeyError:
            pass

        if self.is_alive and hasattr(self.instances.values()[0], attr):
            # Assume homogeneity among instances: if one has it, all do.
            # That is true right now while we have a single REMOTE_CONNECTION_CLASS,
            # provided the user doesn't start hacking around with single instances.
            def _attr_forker(*args, **kwargs):
                work = []
                back_map = []
                for service_info, instance in self.instances.iteritems():
                    current_work = forkjoin.WorkItem(getattr(instance, attr),
                            args, kwargs)
                    work.append(current_work)
                    back_map.append(service_info)

                # maps work idx->result
                results = forkjoin.fork_join(work)

                # maps connection_info->result
                output = {}
                exceptions = {}
                for idx, result in results.iteritems():
                    connection_info = back_map[idx]
                    output[connection_info] = result
                    if isinstance(result, BaseException):
                        exceptions[idx] = result

                assert not exceptions, "ERROR: exceptions while " \
                        "running: %s" % str(exceptions)

                return output

            return _attr_forker

        raise AttributeError(attr)

    @classmethod
    def shutdown_all(cls):
        instances = copy.copy(cls.INSTANCES)
        for instance in instances:
            instance.shutdown()


# atexit hook to ensure we close out all connections. This can help avoid
# deadlocks at exit.
def _clean():
    ServiceHandleArray.shutdown_all()
    ServiceHandler.shutdown_all()

atexit.register(_clean)


