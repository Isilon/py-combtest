import importlib

import combtest.central_logger as central_logger
from combtest.walk import Walk

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
    return _EX_IP.ip

def set_my_IP(ip):
    """
    Args:
        ip as a string.
    """
    _EX_IP.ip = ip


class WalkOpTracer(central_logger.OpTracer):
    def trace(self, **op_info):
        test_file = op_info['test_file']
        walk = op_info['walk']
        walk_id = op_info['walk_id']
        file_config = op_info['file_config']

        info = {'walk_id': walk_id,
                'walk': repr(walk),
                'path': test_file.path,
                'lin': test_file.lin,
                'config': file_config}
        super(WalkOpTracer, self).trace(**info)


class RangeTree(object):
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
    return cls.__module__ + "." + cls.__name__

def get_class_from_qualname(name):
    tokens = name.split(".")
    module_name = ".".join(tokens[:-1])
    class_name = tokens[-1]

    module = importlib.import_module(module_name)
    class_ref = getattr(module, class_name)
    return class_ref


class CtxTypeFile(dict):
    """
    Elements:
        test_file - a test_file.TestFile
        target_range - something that directs the attention of the next
                     action. e.g. this may be a ref to a PG that the
                     next action should operate on.
    """
    def __init__(self, test_file, target_type=None, target_range=None):
        self['test_file'] = test_file
        self['target_type'] = target_type
        self['target_range'] = target_range
        self['walk'] = Walk()
        super(CtxTypeFile, self).__init__()

    @property
    def test_file(self):
        return self['test_file']

    @test_file.setter
    def test_file(self, value):
        self['test_file'] = value

    @property
    def target_type(self):
        return self['target_type']

    @target_type.setter
    def target_type(self, value):
        self['target_type'] = value

    @property
    def target_range(self):
        return self['target_range']

    @target_range.setter
    def target_range(self, value):
        self['target_range'] = value

    @property
    def walk(self):
        return self['walk']

    def update_walk(self, walk_segment, sync_point=None):
        if sync_point is not None:
            self.walk.append(sync_point)
        self.walk.extend(walk_segment)
