"""
SSH-based connection for bootstrapping remote rpyc services. The user does not
have to use this, but it is provided as a default implementation for those not
wanting to write their own connection logic.
"""
import copy

import paramiko

from six import string_types

import combtest.bootstrap as bootstrap
from combtest.central_logger import logger
import combtest.config as config
import combtest.utils as utils


TERM_SIZE = 999

class ServiceHandler_SSH(bootstrap.ServiceHandler):
    """
    Provide an SSH-based method of bootstrapping our rpyc ``Walk`` executors.

    :param ConnectionInfo connection_info: a :class:`combtest.bootstrap.ConnectionInfo`
                                           which optionally provides a
                                           ``service_info`` dictionary
                                           containing SSH authentication
                                           info. You can get such a dict from
                                           :func:`combtest.config.get_ssh_options`
    """
    def __init__(self, connection_info):
        # Populate extra connection info from config files, with the passed
        # in values overriding them.
        ssh_options = copy.copy(config.get_ssh_options())

        if connection_info.service_info:
            ssh_options.update(connection_info.service_info)
        connection_info.service_info = ssh_options

        super(ServiceHandler_SSH, self).__init__(connection_info)

        self.session = None
        self.transport = None
        self.ssh_client = None

    def _reset(self):
        self.session = None
        self.transport = None
        self.ssh_client = None

    def cmd(self, cmd, newline=True):
        if newline:
            cmd += "\n"
        assert len(cmd) < TERM_SIZE
        self.session.sendall(cmd)

    def _ssh_shutdown(self, hard=False):
        if self.session is None:
            if hard:
                # We have nothing to shutdown, because e.g. spawn was never
                # called.
                return
            raise RuntimeError("ERROR: session already shutdown, "
                               "or never started")

        # XXX note that this does *not* signal for a clean shutdown.
        # This ServiceHandler is totally agnostic to the stuff running on
        # the remote side. Up to the user to send a shutdown rpyc message,
        # or send my_ssh_handler.cmd(signal.SIGINT) or some such.
        self.ssh_client.close()
        self._reset()

    # One can inherit and build out extra kwargs if desired
    def _ssh_spawn(self, ip, username=None, password=None, rsakey=None,
                   **connection_info):
        if self.session is not None:
            raise RuntimeError("ERROR: session already spawned")

        if rsakey is not None:
            # RSA key auth, which requires an RSA key instead of a password
            rsakey_handle = paramiko.RSAKey.from_private_key_file(rsakey)

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if rsakey is not None:
            ssh_client.connect(ip, username=username, pkey=rsakey_handle,
                               allow_agent=False)
        else:
            ssh_client.connect(ip, username=username, password=password,
                               allow_agent=False)
        transport = ssh_client.get_transport()
        session = transport.open_session()
        session.setblocking(0)
        session.get_pty(width=TERM_SIZE)
        session.invoke_shell()

        self.session = session
        self.transport = transport
        self.ssh_client = ssh_client

    def start_cmd(self,
                  service_class,
                  service_run_func=bootstrap.DEFAULT_SERVICE_RUN_FUNC):

        if isinstance(service_run_func, string_types):
            service_run_func = utils.get_class_from_qualname(service_run_func)
        srf_module = service_run_func.__module__
        service_run_func = utils.get_class_qualname(service_run_func)

        if not isinstance(service_class, string_types):
            service_qualname = utils.get_class_qualname(service_class)
        else:
            service_qualname = service_class
            service_class = utils.get_class_from_qualname(service_class)
        sc_module = service_class.__module__

        ci_in = {}
        username = None
        password = None
        rsakey = None

        service_info = self._service_info
        for key, value in service_info.items():
            if key == 'username_map':
                if value and self.service_ip in value:
                    username = value[self.service_ip]
            elif key == 'username_override':
                if username is None:
                    username = value
            elif key == 'password_map':
                if value and self.service_ip in value:
                    password = value[self.service_ip]
            elif key == 'password_override':
                if password is None:
                    password = value
            elif key == 'rsakey_map':
                if value and self.service_ip in value:
                    rsakey = value[self.service_ip]
            elif key == 'rsakey_override':
                if rsakey is None:
                    rsakey = value
            elif key not in ('password', 'username', 'rsakey'):
                ci_in[key] = value

        if ('username' in service_info and
            service_info['username'] is not None):
            username = service_info['username']
        if ('password' in service_info and
            service_info['password'] is not None):
            password = service_info['password']
        if ('rsakey' in service_info and
            service_info['rsakey'] is not None):
            rsakey = service_info['rsakey']

        self._ssh_spawn(self.service_ip,
                        username=username,
                        password=password,
                        rsakey=rsakey,
                        **ci_in)

        cmd = ("python -c 'import %s; import %s; %s(\"%s\", %d)'" %
               (srf_module,
                sc_module,
                service_run_func,
                service_qualname,
                self.service_port
               )
              )
        self.cmd(cmd)
        logger.debug("Spawned SSH handles for services")

    def shutdown(self):
        self._ssh_shutdown(hard=True)
        super(ServiceHandler_SSH, self).shutdown()
