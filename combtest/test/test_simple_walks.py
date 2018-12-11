"""
Test basic walks to make sure the walk + range op + param selection stuff
is working.
"""
import json
import os
import rpyc
import threading
import time

import isi.config.array as array
from isi.fs.ifs_types import IFS_BSIZE
from isi.fs.prot_group import PROTECTION_TYPE_MIRRORING, PROTECTION_TYPE_FEC

import filesystem.layout.layout as layout
import filesystem.layout.param_selector as param_selector
import filesystem.layout.range_op_proxy as rop
import filesystem.layout.test_file
import filesystem.layout.walk as walk
import filesystem.layout.worker as worker


class FileCtx(dict):
    """
    This is passed around b/w the ops making up a given walk in order to
    track state. This is the "dynamic context" of the walk.
    """
    def __init__(self, test_file, target_obj=None):
        self['test_file'] = test_file
        self['target_obj'] = target_obj

    @property
    def test_file(self):
        return self['test_file']

    @test_file.setter
    def test_file(self, value):
        self['test_file'] = value

    @property
    def target_obj(self):
        return self['target_obj']

    @target_obj.setter
    def target_obj(self, value):
        self['target_obj'] = value


class Action_SetProt(walk.Action):
    def guess_policy_to_level(self, test_file):
        prot_policy = self.static_ctx
        level = test_file.guess_policy_to_level(prot_policy)
        return level

    def run(self, prot_policy, dynamic_ctx=None):
        dynamic_ctx.test_file.set_prot_pol(prot_policy)

class Action_SelectPGFromFile(walk.Action):
    def run(self, selector, dynamic_ctx=None):
        target_obj = dynamic_ctx.test_file.pgs[selector]
        dynamic_ctx.target_obj = target_obj

class Action_SelectClusterFromPG(walk.Action):
    def __init__(self, *args, **kwargs):
        super(Action_SelectClusterFromPG, self).__init__(*args, **kwargs)
        self.max_idx = None

    def run(self, selectors, dynamic_ctx=None):
        target_pg = dynamic_ctx.target_obj
        length_type, offset_type = selectors
        # TODO: some way to assert correct target_obj type here?

        current_prot = dynamic_ctx.test_file.prot
        if current_prot.type == PROTECTION_TYPE_MIRRORING:
            if offset_type != param_selector.OffsetTypes.OffsetStart:
                raise walk.CancelWalk("Redundant case")
            max_idx = 1
        else:
            max_idx = current_prot.n

        range_selector = param_selector.RangeSelection.random(0,
                max_idx,
                force_length=length_type,
                force_offset=offset_type)
        target_cluster = target_pg.clusters[range_selector.as_slice()]

        dynamic_ctx.target_obj = target_cluster

class Action_SelectChunkFromCluster(walk.Action):
    def run(self, selector, dynamic_ctx=None):
        target_cluster = dynamic_ctx.target_obj

        min_idx = 0
        # (add one since max_idx is right inclusive, but ranges are
        #  generally right exclusive)
        max_idx = target_cluster.chunks.max_idx + 1
        length_type, offset_type = selector

        range_selector = param_selector.RangeSelection.random(min_idx, max_idx,
                force_length=length_type,
                force_offset=offset_type)
        target_chunk = target_cluster.chunks[range_selector.as_slice()]
        dynamic_ctx.target_obj = target_chunk

# NOTE: this is the "arbitrary byte" case, we also want block aligned cases
#       But: this does e.g. do "Write whole chunk", "Write until end of chunk
#       from some offset" etc.
class Action_WriteByteRange(walk.Action):
    def run(self, byte_range_selector, dynamic_ctx=None):
        target_obj = dynamic_ctx.target_obj

        length_type, offset_type = byte_range_selector
        byte_range_selector = \
                param_selector.RangeSelection.random(1,
                        target_obj.end_offset - target_obj.start_offset,
                        force_length=length_type,
                        force_offset=offset_type)

        target_obj[byte_range_selector.as_slice()].write()



def gen_sc():
    protections = ["+4", "+2:1"]
    os_protection = walk.OptionSet(protections, action_class=Action_SetProt)

    pgs = [0, 55, 5000]
    os_pg = walk.OptionSet(pgs, action_class=Action_SelectPGFromFile)

    positional_selector = param_selector.IterTypes(
            lengths=[param_selector.LengthTypes.LengthOne])
    os_cluster = walk.OptionSet(positional_selector,
            action_class=Action_SelectClusterFromPG)

    range_selector = param_selector.IterTypes()
    os_chunk = walk.OptionSet(range_selector,
            action_class=Action_SelectChunkFromCluster)

    range_selector = param_selector.IterTypes()
    os_overwrite = walk.OptionSet(range_selector,
            action_class=Action_WriteByteRange)

    return walk.StateCombinator(os_protection, os_pg, os_cluster, os_chunk,
            os_overwrite)

idx = 0
TEST_DIR = "/ifs/data"
TEST_DIR = os.path.join(TEST_DIR, str(array.getLocalId()))
def run_walks(sc, my_idx, mkdir=False):
    global idx

    if mkdir:
        target_dir = os.path.join(TEST_DIR, str(my_idx))
        try:
            os.makedirs(target_dir)
        except (OSError, IOError):
            pass
    else:
        target_dir = TEST_DIR

    my_start_time = time.time()
    try:
        while True:
            current_walk = sc.pop(0)

            # We don't actually have to make a whole new file here. In fact,
            # that is slow. We probably want 1 for each protection type, and
            # then select different PGs since all of our logic below that
            # level is purely sub-PG. The logic above doesn't limit us from
            # doing this, but let's just keep it simple for now.

            # (idx is racy here, yes, but a given thread will only see a
            #  given idx once. This is just a test. :))
            fname = "blah.%d.%d" % (my_idx, idx)
            fqp = os.path.join(target_dir, fname)
            tf = filesystem.layout.test_file.TestFile(fqp)
            current_walk.execute(FileCtx(tf))
            idx += 1
    except (StopIteration, IndexError):
        pass


# This version serializes walks and send them to the coordinators to keep the
# threads fed. The memory footprint is the size of all the walks in flight. An
# alternative - send info about how to construct a StateCombinator. This would
# reduce the mem footprint on the main coordinator node at least.
if __name__ == "__main__":
    import ssh_session as ssh
    import sys
    from filesystem.layout.walk import WalkExecutorService
    if len(sys.argv) > 1 and sys.argv[1] == "slave":
        print "Starting service"
        worker.start_service(WalkExecutorService)
    else:
        ## First: single threaded sequential
        #sc = gen_sc()
        #start_time = time.time()
        #run_walks(sc, 0)
        #print "Single threaded: Executed %d tests in %0.2fs" % (idx, time.time() -
        #        start_time)


        ## Now let's try parallel, same directory
        #sc = gen_sc()
        #idx = 0
        #threads = []
        #for i in range(50):
        #    t = threading.Thread(target=run_walks, args=(sc, i))
        #    threads.append(t)
        #start_time = time.time()
        #for t in threads:
        #    t.start()
        #for t in threads:
        #    t.join()
        #print "Multithreaded, same dir: Executed %d tests in %0.2fs" % (idx,
        #        time.time() - start_time)


        ## Now let's try parallel, one dir per worker to avoid lock contention on the
        ## dir.
        #sc = gen_sc()
        #idx = 0
        #threads = []
        #for i in range(50):
        #    try:
        #        os.mkdir(os.path.join(TEST_DIR, str(i)))
        #    except OSError:
        #        pass
        #    t = threading.Thread(target=run_walks, args=(sc, i),
        #            kwargs={"mkdir": True})
        #    threads.append(t)
        #start_time = time.time()
        #for t in threads:
        #    t.start()
        #print "Threads started in %0.2fs" % (time.time() - start_time)
        #for t in threads:
        #    t.join()
        #print "Multithreaded, diff dirs: Executed %d tests in %0.2fs" % (idx,
        #        time.time() - start_time)

        #sc = gen_sc()
        #next_walk = sc.next()
        #serialized = json.dumps(next_walk, cls=walk.WalkEncoder)
        #print "Replaying:", serialized
        #deserialized = json.loads(serialized, cls=walk.WalkDecoder)
        #tf = filesystem.layout.test_file.TestFile('/ifs/data/blah')
        #deserialized.execute(FileCtx(tf))


        #for i in range(worker.DEFAULT_MAX_THREAD_COUNT):
        #    try:
        #        os.makedirs('/ifs/data/%d' % i)
        #    except OSError:
        #        pass

        print array.getNodes()
        ips = [node.external() for node in array.getNodes()]
        service_handles = ssh.SSHArray(ips)
        print "Starting services"
        service_handles.start_cmd("python $QA/lib/filesystem/layout/test/test_simple_walks.py slave")
        time.sleep(2)
        print "Services up"
        ips = ['10.111.228.42', '10.111.228.43', '10.111.228.44']

        ctx = {'func': 'filesystem.layout.test.test_simple_walks.run_walks'}
        client_handles = []
        for ip in ips:
            client = rpyc.connect(ip, port=worker.DEFAULT_PORT)
            client_handles.append(client)

        sc = gen_sc()
        work_queues = []
        for _ in range(len(client_handles)):
            work_queues.append([])
        current_q_idx = 0
        for current_walk in sc:
            current_q = work_queues[current_q_idx]
            current_q.append(json.dumps(current_walk, cls=walk.WalkEncoder))
            current_q_idx += 1
            current_q_idx %= len(client_handles)

        print "Work generated; starting timer"

        start_time = time.time()
        current_q_idx = 0
        for client in client_handles:
            current_q = work_queues[current_q_idx]
            print "Queueing", len(current_q)
            client.root.start_workers_on(current_q, ctx=ctx)
            current_q_idx += 1

        for client in client_handles:
            client.root.join_all_workers()

        print "Multinode, same dir/node: %0.2fs" % (time.time() - start_time)
