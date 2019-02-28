"""
We need some way to bootstrap the rpyc-based
:class:`combtest.worker.CoordinatorService`. That can include bootstrapping it
on a remote node if we want to take advantage of a whole cluster. We don't need
to make too many assumptions to do so. Let's provide a general interface for
accomplishing this (:class:`ServiceHandleArray`), and a base implementation for
bootstrapping locally using ``multiprocessing``
(:class:`ServiceHandler_Local`). The client can swap out ``ServiceHandlers`` to
provide alternative bootstrapping logic. ``ServiceHandleArray`` and
``ServiceHandler`` simply provide the interface that a
:class:`combtest.worker.ServiceGroup` uses to bootstrap the service instances.

Note:
    A paramiko-based ``ServiceHandler`` class for SSH connections can be
    found in :class:`combtest.ssh_handle.ServiceHandler_SSH`.
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
# want bootstrap to depend on combtest.worker. This will rarely change. So how
# about we hardcode and let the user override as they see fit?  Unfortunately
# IDEs wont see a reference here (I don't think?). What are alternatives?
DEFAULT_SERVICE_RUN_FUNC = 'combtest.worker.start_service_by_name'


class ConnectionInfo(object):
    """
    This is a simple struct-like object meant to wrap all the stuff needed to
    bootstrap a service.

    :param str ip: A string that can be passed as the first argument to
                   ``rpyc.connect``. Typically this will be an IP address for
                   our purposes, but could also be a hostname.
    :param int port: The port number where the service can be found
    :param object service_info: Additional information your class may need for
                                bootstrapping the service. For the SSH
                                implementation for example, these will be some
                                authentication bits.
    """
    def __init__(self, ip, port, service_info):
        self.ip = ip
        self.port = port
        self.service_info = service_info

    def __repr__(self):
        return str((self.ip, self.port, self.service_info))

    def __str__(self):
        return repr(self)


class ServiceHandler(object):
    """
    A ``ServiceHandler`` implements a way of bootstrapping an rpyc-based
    service. This base class defines the interface and should be overridden.

    :param ConnectionInfo connection_info: Specifies where the service should
                                           be contactable once it is
                                           bootstrapped, and any additional
                                           information needed to bootstrap it.
    """

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
        """
        This method will be called to actually do the bootstrapping work. It
        should be overridden.

        :param <str|rpyc.Service> service_class: A full __qualname__ of the
                                                 service we are bootstrapping,
                                                 or the class itself.
        :param str service_run_func: optional override of the function that
                                     will be used on the remote side to start
                                     the service up. Needs to be a __qualname__.
                                     Typically this can be left default.
        """
        raise NotImplementedError()

    def shutdown(self):
        """
        Called to shut the service down. Should be idempotent unless the class
        writer is *very* careful about how they shut their services down
        manually. The overridden method should call the super-method.
        """
        # (client code to implement actual connection shutdown in subclass)
        self.INSTANCES.remove(self)

    @classmethod
    def shutdown_all(cls):
        instances = copy.copy(cls.INSTANCES)
        for instance in instances:
            instance.shutdown()


class ServiceHandler_Local(ServiceHandler):
    """
    A ServiceHandler implementation foor bootstrapping locally via simple
    multiprocessing.Process call out.
    """
    # In seconds. If this much time or greater goes by during shutdown() we
    # will just point a mean signal at the child proc.
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
    """
    A ServiceHandlerArray is a collection of ServiceHandlers, together with
    dynamic foreach-style dispatch on attribute access. Example:

    >>> sa.ServiceHandleArray(blah blah blah)
    >>> sa.q(1)

    this will dispatch an access to the ``q`` attribute of each contained
    ``ServiceHandler`` in parallel.

    :param iterable connection_infos: An iterable of :class:`ConnectionInfo`
                                      which describe how we will start the
                                      services up.
    :param bool spawn: True if we should spawn the services during
                       initialization, False otherwise. The user can later
                       spawn them manually using :func:`spawn`.
    """

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
        """
        Attach a single instance to this array.
        """
        key = repr(connection_info)
        assert key not in self.instances, "ERROR: we already have an " \
                "instance for %s" % key
        self.instances[key] = instance

    def _dettach(self, key):
        """
        Dettach an instance from this array and return it. This does not close
        the instance; simply removes it from the array. This is a protected
        member to avoid confusion around the meaning of the key, and to enforce
        invariant: that repr(connection_info) is the key we use for an
        instance, since we can't be 100% sure that a ConnectionInfo is
        hashable.
        """
        assert key in self.instances, "ERROR: no such instance %s" % key
        instance = self.instances[key]
        del self.instances[key]
        return instance

    @property
    def is_alive(self):
        """
        :return: True if we have any connections, False otherwise. This is
                 not directly related to whether the services are up; e.g.
                 they may have died for some reason.
        """
        # For now: don't try to check connection states. Let's just check if we
        # have any connections.
        return bool(self.instances)

    def spawn(self, connection_info):
        """
        Spawn a single instance and attach it to this array.

        :param ConnectionInfo connection_info: The ConnectionInfo that
                                               describes how to start this
                                               instance.
        """
        # We assert here in addition to self.attach to prevent even attempting
        # a connection if we already have one, while still having an assert in
        # self.attach for users that are using it directly.
        key = repr(connection_info)
        assert key not in self.instances, "ERROR: we already have an " \
                "instance for %s" % key
        instance = self.REMOTE_CONNECTION_CLASS(connection_info)
        self.attach(connection_info, instance)

    def spawn_many(self, connection_infos=None):
        """
        Spawn and attach a bunch of instances.

        :param iterable connection_infos: if None, we will spawn using the
                                          :class:`ConnectionInfo` s passed to
                                          the initializer. Otherwise,
                                          ``connection_infos`` should be an
                                          iterable of ``ConnectionInfos``.
        """
        work = []

        if connection_infos is not None:
            self._connection_infos = copy.copy(connection_infos)

        for connection_info in self._connection_infos:
            work_item = forkjoin.WorkItem(self.spawn, connection_info)
            work.append(work_item)

        results = forkjoin.fork_join(work)
        for idx, result in enumerate(results):
            if isinstance(result, BaseException):
                connection_info = self._connection_infos[idx]
                sys.stderr.write("ERROR spawning (%s): " %
                        repr(connection_info))
                raise result

    def _shutdown_single(self, key):
        """
        Shutdown a single instance and stop tracking it.

        :raises KeyError: if 'ip' does not correspond to an instance we are
                          tracking.
        """
        instance = self._dettach(key)
        instance.shutdown()

    def shutdown(self, hard=False):
        """
        Shutdown all instances this array is tracking.

        :param bool hard: if False, we will assert that instances shut down
                          cleanly, otherwise we won't.
        """
        work = []
        keys = copy.copy(self.instances.keys())
        for key in keys:
            work_item = forkjoin.WorkItem(self._shutdown_single, key)
            work.append(work_item)

        results = forkjoin.fork_join(work)
        if not hard:
            for idx, result in enumerate(results):
                if isinstance(result, BaseException):
                    key = keys[idx]
                    sys.stderr.write("ERROR shutting down (%s): " % key)
                    raise result

        self.INSTANCES.remove(self)

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
                            *args, **kwargs)
                    work.append(current_work)
                    back_map.append(service_info)

                # maps work idx->result
                results = forkjoin.fork_join(work)

                # maps connection_info->result
                output = {}
                exceptions = {}
                for idx, result in enumerate(results):
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
        """
        Shutdown all arrays which have not yet been shut down.
        """
        instances = copy.copy(cls.INSTANCES)
        for instance in instances:
            instance.shutdown()


# atexit hook to ensure we close out all connections. This can help avoid
# deadlocks at exit.
def _clean():
    ServiceHandleArray.shutdown_all()
    ServiceHandler.shutdown_all()

atexit.register(_clean)


