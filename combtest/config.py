"""
Getters, setters, and loaders of config. Config includes e.g. SSH
authentication stuff, port numbers to use, etc.

There are two layers of config provided: config loaded from file via
``refresh_cfg``, and runtime-provided overrides to those. The getters given
below will enforce that ordering. The user provides overrides via the setters.
"""

import ConfigParser
import errno
import json
import multiprocessing


#: Location of a config file to load, if the user wants to load config values
#: that way
CONFIG_LOC = "combtest.cfg"

# Keys:
# ssh_rsakey_override: single rsa key file path
# ssh_rsakey_overrides: json formatted mapping ip->rsa key file path
SSH_CONF = {}

# Keys:
# machine_ips: list of strings
# service_port, logger_port: port nums for walk running service and logger
NET_CONF = {}

# Worker-related config (e.g. number of worker threads)
WORKER_CONF = {}
# Keys:
# Same as WORKER_CONF.
# This is a per-key override of WORKER_CONF that can be set at runtime.
# The reason I don't modify WORKER_CONF is that it can be overridden
# when refresh_cfg() is called. I want the runtime-provided overrides to
# persist and lay "over top" of the config loaded from file. Probably I
# should make a clean "ConfigCache" object for this instead of using a pair
# of dicts?
WORKER_CONF_OVERRIDE = {}


#: Default port number for interproc communication for the logger
DEFAULT_LOGGER_PORT = 6186

#: Default port an rpyc service should listen on e.g. if we start up a remote
#: service
DEFAULT_SERVICE_PORT = 6187

#: Default number of workers executing a ``combtest.walk.Walk`` at a time
#: for a given service instance.
DEFAULT_MAX_THREAD_COUNT = multiprocessing.cpu_count() * 3
# ^^ This is an arbitrary target. The idea is that we can't really use our
# resources with just cpu_count threads, since we may spend a ton of time
# sleeping on I/O. User can tweak.


def refresh_cfg():
    """
    Call to refresh config loaded from file.
    """
    try:
        with open(CONFIG_LOC, 'r') as f:
            parser = ConfigParser.SafeConfigParser()
            parser.readfp(f)

            sections = parser.sections()

            if 'SSH' in sections:
                for key, value in parser.items('SSH'):
                    if key == 'rsakey':
                        SSH_CONF['ssh_rsakey_override'] = value
                    elif key == 'rsakey_map':
                        overrides = json.loads(value)
                        SSH_CONF['ssh_rsakey_map'] = overrides
                    elif key == 'username':
                        SSH_CONF['ssh_username_override'] = value
                    elif key == 'username_map':
                        SSH_CONF['ssh_username_map'] = value
                    elif key == 'password':
                        SSH_CONF['ssh_password_override'] = value
                    elif key == 'password_map':
                        SSH_CONF['ssh_password_map'] = value
                    else:
                        raise KeyError("Key not regonized: " + str(key))

            if 'NET' in sections:
                for key, value in parser.items('NET'):
                    if key == 'machine_ips':
                        NET_CONF['machine_ips'] = [ip.strip() for ip in
                                value.split(',')]
                    elif key == 'service_port':
                        NET_CONF['service_port'] = int(value)
                    elif key == 'logger_port':
                        NET_CONF['logger_port'] = int(value)
                    else:
                        raise KeyError("Key not regonized: " + str(key))

            if 'WORKER' in sections:
                for key, value in parser.items('WORKER'):
                    if key == 'max_thread_count':
                        WORKER_CONF['max_thread_count'] = int(value)
                    else:
                        raise KeyError("Key not regonized: " + str(key))

    except IOError as e:
        if e.errno != errno.ENOENT:
            raise

def get_ssh_rsa_keys():
    """
    Get paths to RSA keys that we can use for SSH authentication. This can
    include a single file, or one file per e.g. IP.
    :return: path, map of ip->path
    """
    override = SSH_CONF.get('ssh_rsakey_override', None)
    override_map = SSH_CONF.get('ssh_rsakey_map', None)

    return (override, override_map)

def get_ssh_usernames():
    """
    Get usernames for SSH authentication.
    :return: username, map of ip->username
    """
    override = SSH_CONF.get('ssh_username_override', None)
    override_map = SSH_CONF.get('ssh_username_map', None)

    return (override, override_map)

def get_ssh_passwords():
    """
    Get passwords for SSH authentication.
    :return: password, map of ip->password
    """
    override = SSH_CONF.get('ssh_password_override', None)
    override_map = SSH_CONF.get('ssh_password_map', None)

    return (override, override_map)

def get_ssh_options():
    """
    Get all SSH authentication options in a single call.
    :returns: a dict of options
    """
    rsakey_override, rsakey_map = get_ssh_rsa_keys()
    username_override, username_map = get_ssh_usernames()
    password_override, password_map = get_ssh_passwords()

    return {'rsakey_override': rsakey_override,
            'rsakey_map': rsakey_map,
            'username_override': username_override,
            'username_map': username_map,
            'password_override': password_override,
            'password_map': password_map,
           }

def get_machine_ips():
    """
    Get a list of IPs where we should try to set services up
    """
    return NET_CONF.get('machine_ips', [])

def set_service_port(port):
    """
    Set the port number where we should expect to find an rpyc service running,
    once it is bootstrapped.
    """
    WORKER_CONF_OVERRIDE['service_port'] = port

def get_service_port():
    """
    Get the port number where we should expect to find an rpyc service running,
    once it is bootstrapped.
    """
    port = WORKER_CONF_OVERRIDE.get('service_port', None)
    if port is None:
        port = WORKER_CONF.get('service_port', DEFAULT_SERVICE_PORT)
    return port

def get_logger_port():
    """
    Get the port we will expose locally for remote loggers to connect to.
    """
    return WORKER_CONF.get('logger_port', DEFAULT_LOGGER_PORT)

def get_max_thread_count():
    """
    Get the max thread count for :class:`combtest.worker.ThreadPool`.
    """
    return WORKER_CONF.get('max_thread_count', DEFAULT_MAX_THREAD_COUNT)
