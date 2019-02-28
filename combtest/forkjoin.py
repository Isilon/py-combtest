"""
Simple fork/join implementation, modelled after Java's lib for the same. This
doesn't attempt to make any scheduling optimizations for e.g. the sake of
minimizing makespan. Threads simply share a queue and pop work off of it until
the queue is drained.

Note:
    This is not designed well for bytecode-level work. This is for I/O heavy
    work like e.g. performing network roundtrips, doing disk I/O, etc. If you
    need to dodge the GIL, you'll want to use something else.
"""

from collections import namedtuple
import copy
import sys
import threading
import time
import traceback


# Max simultaneous outstanding threads. 60 happens to be a common default
# number of SSH connections, which is likely to be a common way of
# bootstrapping, so let's take that as a semi-arbitrary default.
MAX_SPAWN_THREADS = 60


class WorkItem(object):
    """
    A simple container representing a work item to be done.

    :param function func: A pointer to the work callback
    :param args: args to pass to the work callback
    :param kwargs: kwargs to pass to the work callback
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

# We will use this to wrap our threads, work items, and results together for
# tracking.
_RunningWorkItem = namedtuple("_RunningWorkItem", "thread work_item result")

def fork_join(work,
              suppress_errors=False,
              max_spawn_threads=MAX_SPAWN_THREADS):
    """
    Poor man's fork/join implementation.  Run all work items in parallel up to some
    maximum number of simultaneous threads.  Wait for all work to finish, then
    return results.  Maximum number of threads that will run in parallel
    for a given job will be max_spawn_threads.

    :param iterable work: an iterable of WorkItems that supports direct
                          indexing and len

    :param bool suppress_errors: if False, we will print any exceptions to
                                 stderr, otherwise we won't. Does not affect
                                 the return value.

    :param int max_spawn_threads: The max number of threads running
                                  simultaneously.

    :return: A list of returned items, in 1:1 correspondence to the WorkItems
             in 'work'. That is: the first result corresponds to the first
             WorkItem, etc. If nothing is explicitly returned by an item,
             its 'return value' in this list will be None.
    """
    def work_runner(func, args, kwargs, result_container):
        # Inner func we use to catch exceptions
        try:
            return_item = func(*args, **kwargs)
        except Exception as e:
            tb = traceback.format_exc()
            result_container.append(e)
            return_item = tb
        result_container.append(return_item)

    returned_items = {}
    running_work = []
    remaining_work = copy.copy(work)
    while remaining_work or running_work:
        # Loop over running threads to collect any that have finished
        # TODO: whack threads after some timeout and then abort
        new_running_work = []
        for running_item in running_work:
            running_work_idx = running_item[0]
            running_work_item = running_item[1]
            running_thread = running_work_item.thread
            if running_thread.is_alive() and not running_work_item.result:
                new_running_work.append(running_item)
            else:
                if running_work_item.result:
                    returned_item = running_work_item.result[0]

                    if not suppress_errors and \
                            len(running_work_item.result) > 1 and \
                            isinstance(returned_item, BaseException):
                        sys.stderr.write(str(running_work_item.work_item) +
                                "\n")
                        sys.stderr.write(running_work_item.result[1] + "\n")
                else:
                    returned_item = None

                returned_items[running_work_idx] = \
                        returned_item
        running_work = new_running_work

        if len(running_work) < max_spawn_threads and remaining_work:
            idx = len(remaining_work) - 1
            work_item = remaining_work.pop()
            result_container = []
            t = threading.Thread(target=work_runner, args=(work_item.func,
                    work_item.args, work_item.kwargs, result_container))
            t.start()
            running_work.append((idx, _RunningWorkItem(t, work_item,
                    result_container)))
        else:
            time.sleep(1)

    # There is no reason to have a hash here since we are indexed on
    # consecutive integers. So let's coerce this guy to a list.
    out = [None] * len(work)
    for idx in range(len(work)):
        out[idx] = returned_items[idx]
    return out
