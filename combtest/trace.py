import combtest.encode as encode

INSTANCES = set()


class JsonTracer(object):
    """A Tracer creates a record of each op run.
    """
    def __init__(self, log_func, append_newline=False):
        """Args:
            path - the path where we will write the trace
        """
        self.log_func = log_func
        self.append_newline = append_newline
        INSTANCES.add(self)

    def trace(self, namespace, record, verifies):
        string = encode.encode((namespace, record, verifies))
        if self.append_newline:
            string += "\n"
        self.log_func(string)

    def finalize(self):
        if self in INSTANCES:
            INSTANCES.remove(self)
