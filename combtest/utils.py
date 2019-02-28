"""
Misc utils
"""

import importlib
import socket


# Use this to override the IP we will use
class _ExIP(object):
    def __init__(self):
        self.ex_ip = None

    @property
    def ip(self):
        if self.ex_ip is None:
            # Get my local IP address that would be used by default to ping
            # out to the internet.  I'm sure there are a variety of cases
            # this doesn't cover, but it works for now, and is platform
            # independent.
            s4 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s4.connect(("1.1.1.1", 1))
                ip = str(s4.getsockname()[0])
            except socket.error:
                s6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
                try:
                    s6.connect(("0:0:0:0:0:ffff:101:101", 1))
                    ip = str(s6.getsockname()[0])
                except socket.error:
                    # Loopback only?
                    ip = '127.0.0.1'
                finally:
                    s6.close()
            finally:
                s4.close()

            self.ex_ip = ip

        return self.ex_ip

    @ip.setter
    def ip(self, ip):
        if not isinstance(ip, basestring):
            raise ValueError("IP must be formatted as a string")

        self.ex_ip = ip

_EX_IP = _ExIP()

def get_my_IP():
    """
    Get our best guess at one of our local IPs that others could use to contact
    us (e.g. to connect to a local log server).
    """
    return _EX_IP.ip

def set_my_IP(ip):
    """
    Set the IP that the rest of the system should assume is our local IP
    address. This may work for a hostname as well, but that isn't well tested.

    :param str ip: ip as a string
    """
    _EX_IP.ip = ip


class RangeTree(object):
    """
    A representation of a range, with the levels of the tree representing
    hierarchical splits of the range.
    O(N) mem in the number of sub-ranges.
    Example::
     -->972
       -->486
         -->243
         -->243
       -->486
         -->243
         -->243
    """
    class Node(object):
        def __init__(self, max_idx, value):
            self.max_idx = max_idx
            self.value = value
            # Items: (max_idx, Node)
            self.children = []

        def split(self, max_idxs, values):
            assert len(max_idxs) == len(values)

            new_children = []
            for idx, value in enumerate(values):
                new_value = self.value
                if value is not None:
                    new_value += (value,)
                max_idx = max_idxs[idx]
                assert max_idx <= self.max_idx
                child = RangeTree.Node(max_idx, new_value)
                new_children.append(child)

            self.children.extend(new_children)
            return new_children

    def __init__(self, min_idx, max_idx, root_value=()):
        self.min_idx = min_idx
        self.max_idx = max_idx
        self._root = RangeTree.Node(max_idx, root_value)

    def split(self, max_idxs, values):
        return self._root.split(max_idxs, values)

    def provide(self, idx):
        node = self._root

        while node.children:
            for child in node.children:
                if idx < child.max_idx:
                    node = child
                    break

        return node.value


def get_class_qualname(cls):
    """
    Give a fully qualified name for a class (and therefore a function ref as
    well). Example: 'combtest.action.Action'.

    Note:
        Python3 has this, or something like it, called ``__qualname__``. We
        implement our own here to retain control of it, and give us Python2
        compat.
    """
    return cls.__module__ + "." + cls.__name__

def get_class_from_qualname(name):
    """
    Resolve a fully qualified class name to a class, attempting any imports
    that need to be done along the way.
    :raises: ImportError or AttributeError if we can't figure out how to import it
    """
    tokens = name.split(".")
    module_name = ".".join(tokens[:-1])
    class_name = tokens[-1]

    module = importlib.import_module(module_name)
    class_ref = getattr(module, class_name)
    return class_ref

