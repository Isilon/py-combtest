"""
Central logger for the system, which includes heavy buffering to help with
scalability. Logging millions of case starts/ends would be too heavy of a cost
without some buffering I think.
"""
import atexit
import cPickle
import copy
import logging
from logging import ERROR, WARNING, INFO, DEBUG
import logging.handlers
from logging.handlers import MemoryHandler
import os
import SocketServer
import struct
import sys
import threading
import time

import combtest.encode as encode
from combtest.utils import get_my_IP

# Everything will ultimately log to this
BASE_LOGGER_NAME = "combtest"
base_logger = logging.getLogger(BASE_LOGGER_NAME)
# The level is controlled here instead of in the handlers, which should all be
# set to DEBUG.
base_logger.setLevel(DEBUG)
logger_formatter = logging.Formatter('%(asctime)s: %(levelname)s: '
                                     '%(filename)s:%(lineno)s: '
                                     '%(message)s')

# "Status" level log lines should always appear in the log, so they need a high
# priority, but they should not be as scary sounding as "Error".
STATUS_LEVELV_NUM = ERROR + 1
logging.addLevelName(STATUS_LEVELV_NUM, "STATUS")

# "Op" level log lines are similar, but debug level.
OP_LEVELV_NUM = DEBUG
logging.addLevelName(OP_LEVELV_NUM, "OP")

OP_LEVEL_PRINT_NUM = STATUS_LEVELV_NUM
logging.addLevelName(OP_LEVEL_PRINT_NUM, "OP")

# So we can reset capacity on request
MEM_HANDLERS = []


class OpTracer(object):
    """
    Used to JSONify ops and put them on disk, with immediate flush in case of a
    panic.
    """
    def __init__(self, log_dir, *namespace):
        if namespace:
            fname = str(namespace[0])
            for item in namespace[1:]:
                fname += ".%s" % str(item)
            fname += "."
        else:
            fname = ""
        fname += str(time.time()) + ".trace"
        self._fname = os.path.join(log_dir, fname)
        self._handle = os.open(self._fname, os.O_WRONLY | os.O_CREAT)
        atexit.register(self.finalize)

    @property
    def fname(self):
        return self._fname

    @property
    def handle(self):
        return self._handle

    def finalize(self):
        try:
            os.close(self._handle)
        except OSError:
            pass

    def write(self, info):
        os.write(self._handle, info)
        os.fsync(self._handle)

    def trace(self, **op_info):
        info = encode.encode(op_info) + "\n"
        self.write(info)

def log_op(*args, **kwargs):
    """
    Similar to logger.<level>(), but for the "op" level.
    """
    logger.log_net(OP_LEVELV_NUM, *args, **kwargs)

def log_local_op(*args, **kwargs):
    logger.log(OP_LEVEL_PRINT_NUM, *args, **kwargs)

def log_status(*args, **kwargs):
    """
    Similar to logger.<level>(), but for the "status" level.
    """
    logger.log_net(STATUS_LEVELV_NUM, *args, **kwargs)

def log_remote_error(*args, **kwargs):
    logger.log_net(ERROR, *args, **kwargs)

def set_level(level):
    """
    Set the log level of the base logger.
    Args:
        level - one of logging.<level>
    """
    base_logger.setLevel(level)

def set_mem_handler_capacities(capacity):
    for handler in MEM_HANDLERS:
        handler.capacity = capacity

def add_handler(handler):
    """
    Add a new handler, wrapped in a MemoryHandler for buffering.
    """
    # Use MemoryHandler as an intermediary for buffering. Essentially: we don't
    # want to flush every message, since that presents a severe scalability
    # issue.
    mem_handler = MemoryHandler(32, target=handler)
    mem_handler.setFormatter(logger_formatter)
    # Real filtering will be done by the logger itself
    mem_handler.setLevel(DEBUG)
    MEM_HANDLERS.append(mem_handler)
    base_logger.addHandler(mem_handler)

def add_file_handler(path):
    """
    Add a file handler, pointed at the file at the given path.
    """
    file_handler = logging.FileHandler(path)
    # Real filtering will be done by the logger itself
    file_handler.setLevel(DEBUG)
    add_handler(file_handler)

def add_op_trace(log_dir, trace_class, path=None):
    """
    Args:
        trace_class - an OpTracer
        path - broken up to make a namespace, which is handy since we often
               have some 'path' associated with a test that clear marks its
               identity, such as /ifs/data/my_test_name
    """
    if path:
        path = str(path).split(os.path.sep)
        # leading slash case
        if not path[0]:
            path = path[1:]
    else:
        path = tuple()
    logger.attach_op_trace(log_dir, trace_class, *path)

# Handler to make sure we get the output to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
# Real filtering will be done by the logger itself
stdout_handler.setFormatter(logger_formatter)
stdout_handler.setLevel(DEBUG)
add_handler(stdout_handler)


# Indirection from the exposed object so it is easy to replace 'logger' with a
# ref to something other than base_logger at some point.
class LoggerProxy(object):
    def __init__(self, logger):
        self.logger = logger
        self.op_trace = None
        self.net_logger = None

    def __getattr__(self, key):
        if key in ("error", "logger","net_logger", "log_net", "op_trace"):
            return self.__dict__[key]
        return getattr(self.__dict__['logger'], key)

    def __setattr__(self, key, value):
        if key in ('logger', 'net_logger', 'op_trace'):
            self.__dict__[key] = value
        else:
            setattr(self.__dict__['logger'], key, value)

    def attach_op_trace(self, log_dir, trace_class, *namespace):
        trace = trace_class(log_dir, *namespace)
        self.op_trace = trace

    def dettach_op_trace(self):
        if self.op_trace is None:
            raise RuntimeError("We don't have an op trace to dettach")

        self.op_trace.finalize()
        self.op_trace = None

    def trace_op(self, **op_info):
        if self.op_trace is not None:
            self.op_trace.trace(**op_info)
        else:
            # Log to debug() only if we aren't tracing. The trace data is
            # truely huge, so it doesn't seem wise to double log. Maybe
            # this should be a tunable. Somebody file an issue :)
            self.debug("%s", str(op_info))

    def error(self, *args, **kwargs):
        self.log_net(ERROR, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        if self.net_logger is not None:
            self.net_logger.exception(msg, *args, **kwargs)
        self.logger.exception(msg, *args, **kwargs)

    def log_net(self, level, *args, **kwargs):
        if self.net_logger is not None:
            self.net_logger.log(level, *args, **kwargs)
        self.logger.log(level, *args, **kwargs)
logger = LoggerProxy(base_logger)


# Socket stuff here down is adapted from the Python2 cookbook:
# https://docs.python.org/2/howto/logging-cookbook.html#sending-and-receiving-logging-events-across-a-network
# Accessed 03/15/18

class NetLogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '%s: %s' % (self.extra['my_ip'], msg), kwargs

def add_socket_handler(host, port):
    """
    Host and port should point back to the logging port of the test
    coordinator (that is - the machine where e.g. a ServiceGroup is running).
    """
    if isinstance(logger.net_logger, NetLogAdapter):
        return

    socket_handler = logging.handlers.SocketHandler(host, port)

    net_logger = logging.getLogger(BASE_LOGGER_NAME + ".net")

    # Don't bother with a formatter, since a socket handler sends the event as
    # an unformatted pickle
    net_logger.addHandler(socket_handler)

    my_ip = get_my_IP()
    nla = NetLogAdapter(net_logger, {'my_ip': my_ip})

    logger.net_logger = nla

class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
    """Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally.
    """

    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        chunk = self.request.recv(4)
        if len(chunk) < 4:
            return
        slen = struct.unpack(">L", chunk)[0]
        chunk = self.request.recv(slen)
        while len(chunk) < slen:
            chunk = chunk + self.request.recv(slen - len(chunk))
        obj = self.unPickle(chunk)
        record = logging.makeLogRecord(obj)
        self.handleLogRecord(record)

    def unPickle(self, data):
        return cPickle.loads(data)

    def handleLogRecord(self, record):
        logger = self.server.logger
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)

class LogRecordSocketReceiver(SocketServer.ThreadingTCPServer):
    """simple TCP socket-based logging receiver suitable for testing.
    """

    allow_reuse_address = 1

    def __init__(self, host, port, handler=LogRecordStreamHandler):
        SocketServer.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logger = logger.logger

    def serve_until_stopped(self):
        import select
        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()],
                                       [], [],
                                       self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort

def logger_recv_main(host, port, parent):
    parent.server = LogRecordSocketReceiver(host=host, port=port)
    parent.server.serve_until_stopped()

class LogRecvr(object):
    INSTANCES = set()
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._thread = None
        self.server = None
        LogRecvr.INSTANCES.add(self)
        self.start()

    def start(self):
        self._thread = threading.Thread(target=logger_recv_main,
                                        args=(self._host, self._port, self))
        self._thread.start()

    def stop(self):
        if self.server is not None:
            self.server.abort = 1

        if self._thread is not None:
            self._thread.join()

        LogRecvr.INSTANCES.discard(self)

def start_recv_remote_logs(host, port):
    # (ref is maintained by class static set)
    LogRecvr(host=host, port=port)

def stop_recv_remote_logs():
    _clean_log_recvr()

@atexit.register
def _clean_log_recvr():
    import copy
    instances = copy.copy(LogRecvr.INSTANCES)
    for instance in instances:
        instance.stop()
