"""
SSH-based connection for bootstrapping remote RPYc services. The user does not
have to use this, but it is provided as a default implementation for those not
wanting to write their own connection logic.
"""
import combtest.remote as remote

import paramiko


class SSHRemote(remote.Remote):
    def __init__(self, connection_info):
        ...

    def start_cmd(self, service_info):
        raise NotImplementedError()

    def shutdown(self):
        # (client code to implement actual connection shutdown in subclass)
        CONNECTION_INSTANCES.remove(self)


