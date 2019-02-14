from collections import namedtuple
import copy
import sys
import threading
import time
import traceback


# Max simultaneous outstanding threads. This happens to correspond to the
# default number of max ssh connections, which is a common use of this
# framework.
MAX_SPAWN_THREADS = 60


WorkItem = namedtuple("WorkItem", "func args kwargs")
RunningWorkItem = namedtuple("RunningWorkItem", "thread work_item result")
def fork_join(work, suppress_errors=False):
    """
    Poor man's fork/join framework.  Run all work items in parallel up to some
    maximum number of simultaneous threads.  Wait for all work to finish, then
    return results.  Maximum number of threads that will run in parallel will
    be MAX_SPAWN_THREADS.
    Args:
        work - an iterable of WorkItems that supports direct indexing and len
    Returns:
        Hash idx of WorkItem->returned item from func
    """
    def work_runner(func, args, kwargs, result_container):
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

        if len(running_work) < MAX_SPAWN_THREADS and remaining_work:
            idx = len(remaining_work) - 1
            work_item = remaining_work.pop()
            result_container = []
            t = threading.Thread(target=work_runner, args=(work_item.func,
                    work_item.args, work_item.kwargs, result_container))
            t.start()
            running_work.append((idx, RunningWorkItem(t, work_item,
                    result_container)))
        else:
            time.sleep(1)

    return returned_items

