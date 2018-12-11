from collections import namedtuple
import copy
import rpyc
import time

import combtest.worker as worker
import combtest.layout.utils as utils


Work = namedtuple("Work", "func_name arg")

def echo(arg=None, ctx=None):
    """
    We "echo" the arg back into the ctx so when our remote caller comes to pick
    the ctx back up they see we have actually done some work.
    """
    if arg is None:
        arg = 1

    if (arg % 10000) == 0:
        print "Running", arg

    return arg

class SimpleRunner(object):
    def __init__(self, arg, resp):
        self._arg = arg
        self._my_dict = resp

    def __call__(self, ctx):
        resp = echo(self._arg)
        self._my_dict[resp] = resp

class SimpleImportService(worker.CoordinatorService):
    def work_repack(self, work, ctx=None, resp=None):
        work = Work(*work)
        class_ref = utils.get_class_from_qualname(work.func_name)
        work_out = class_ref(work.arg, resp)
        return work_out

if __name__ == "__main__":
    this_module = "combtest.test.test_worker."
    sg = worker.ServiceGroup(this_module + SimpleImportService.__name__)

    # Test sending a batch of work
    WORK_SIZE = 100006
    print "Test: scattering %d units of work" % WORK_SIZE
    a_bunch_of_work = []
    for i in range(WORK_SIZE):
        current_work = Work(this_module + SimpleRunner.__name__, i)
        a_bunch_of_work.append(tuple(current_work))

    start_time = time.time()
    worker_ids = sg.scatter_work(a_bunch_of_work, ctx={})
    print "scatter took", time.time() - start_time
    sg.join()

    master = {}
    start_time = time.time()
    for ip, worker_id in worker_ids.iteritems():
        resp = sg.gather_resp(ip, worker_id)
        d_resp = rpyc.utils.classic.obtain(resp)
        master.update(d_resp)
    print "resp retrieval took", time.time() - start_time
    print "total", len(master)
    assert set(master.keys()) == set(range(WORK_SIZE))


    # Now test starting all threads on a single work item. Used e.g. to send a
    # fuzz recipe that all threads should execute.
    print "\nTest: get all nodes running the same unit of work"
    new_ctx = {}
    work = Work(this_module + SimpleRunner.__name__, None)
    work = tuple(work)
    start_time = time.time()
    worker_ids = sg.start_all_on(work, shared_ctx=new_ctx)
    print "starting the job took", time.time() - start_time
    sg.join()

    start_time = time.time()
    master = {}
    for ip, worker_id in worker_ids.iteritems():
        resp = sg.gather_resp(ip, worker_id)
        master.update(resp)
    print "resp retrieval took", time.time() - start_time
    print "resp is", master
    assert set(master.keys()) == set(range(1, len(array.getNodes()) + 1))

    print "\nTest: shutdown + restart"
    sg.shutdown()
    sg.spawn()

    print "\nTest: Synchronous run of work batch"
    responses = sg.run([work, work], ctx={})
    print "resp is", responses

