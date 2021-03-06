from collections import namedtuple
import copy
import rpyc
import time

import combtest.encode as encode
import combtest.worker as worker
import combtest.utils as utils


def echo(arg=None, state=None):
    """
    We "echo" the arg back into the state so when our remote caller comes to pick
    the state back up they see we have actually done some work.
    """
    if arg is None:
        arg = 1

    if (arg % 10000) == 0:
        print("Running", arg)

    return arg

class SimpleRunner(object):
    def __init__(self, arg):
        self._arg = arg

    def __call__(self, state):
        resp = echo(self._arg)
        state[resp] = resp

    def to_json(self):
        return self._arg, self._my_dict

    @classmethod
    def from_json(cls, obj):
        return cls(*obj)

class SimpleImportService(worker.CoordinatorService):
    def work_repack(self, work, state, resp=None):
        work = int(work)
        work_out = SimpleRunner(work)
        return work_out

if __name__ == "__main__":
    this_module = "combtest.test.test_worker."
    my_ip = utils.get_my_IP()
    sg = worker.ServiceGroup(this_module + SimpleImportService.__name__)

    # Test sending a batch of work
    WORK_SIZE = 100006
    print("Test: scattering %d units of work" % WORK_SIZE)
    a_bunch_of_work = range(WORK_SIZE)

    start_time = time.time()
    worker_ids = sg.scatter_work(a_bunch_of_work, state={})
    print("scatter took", time.time() - start_time)
    sg.join()

    master = {}
    start_time = time.time()
    for con, worker_id in worker_ids.items():
        state = sg.gather_state(con, worker_id)
        d_state = rpyc.utils.classic.obtain(state)
        master.update(d_state)
    print("resp retrieval took", time.time() - start_time)
    print("total", len(master))
    assert set(master.keys()) == set(range(WORK_SIZE))

    # Now test starting all threads on a single work item. Used e.g. to send a
    # fuzz recipe that all threads should execute.
    print("\nTest: get all nodes running the same unit of work")
    start_time = time.time()
    work = 1
    worker_ids = sg.start_all_on(work, shared_state={})
    print("starting the job took", time.time() - start_time)
    sg.join()

    start_time = time.time()
    master = {}
    for con, worker_id in worker_ids.items():
        resp = sg.gather_state(con, worker_id)
        master.update(resp)
    print("resp retrieval took", time.time() - start_time)
    print("resp is", master)
    assert master == {1: 1}

    print("\nTest: shutdown + restart")
    sg.shutdown()
    sg.spawn()

    print("\nTest: Synchronous run of work batch")
    responses = sg.run([work, work], state={})
    print("resp is", responses)

