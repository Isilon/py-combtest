"""
Test walk replay mechanisms
"""
import os
import random
import shutil
import tempfile
import unittest

from combtest.action import OptionSet, SyncPoint
import combtest.central_logger as central_logger
import combtest.encode as encode
import combtest.replay as replay
import combtest.runner as runner
import combtest.walk as walk
from combtest.utils import get_my_IP

import combtest.test.classes.actions as actions

MY_IP = get_my_IP()

def random_option(option_set):
    options = [option for option in option_set]
    return random.choice(options)

#class TestReplayByLib(unittest.TestCase):
#    """
#    This is pretty much just a local 'run walk' function. Let's get code
#    coverage of it anyway.
#    """
#
#    def _get_random_walk(self):
#        s1 = random_option(actions.SyncPointAppend1.get_option_set())
#        a1 = random_option(actions.ActionAppend1.get_option_set())
#        a2 = random_option(actions.ActionAppend1.get_option_set())
#
#        test_walk = walk.Walk([s1, a1, a2])
#        return test_walk
#
#    def test_walk_replay(self):
#        test_walk = self._get_random_walk()
#
#        ctx = {}
#        canceled = runner.replay_multistage_walk(test_walk, ctx=ctx)
#        self.assertFalse(canceled)
#
#
#        result = ctx['inner']
#        expected = [a.static_ctx for a in test_walk]
#        self.assertEquals(result, expected)

class TestReplayFromTrace(unittest.TestCase):
    def test_walk_replay(self):
        log_dir = tempfile.mkdtemp()

        walk_order = [actions.ActionSingleton1,
                      actions.ActionSingleton2,
                      actions.SyncPointSingleton1,
                      actions.ActionSingleton3]
        # See classes.py
        expected = [1, 2, 0, 3]

        ctx = {}
        walk_count, error_count, _, _, ctxs, logs = \
                runner.run_multistage_walks(walk_order, verbose=True,
                log_dir=log_dir,
                ctx=ctx,
                gather_ctxs=True)
        self.assertEquals(walk_count, 1)
        self.assertEquals(error_count, 0)

        self.assertEquals(len(ctxs), 1)
        ctxs = ctxs[0]
        self.assertEquals(len(ctxs), 1)
        ctx = ctxs.values()[0]
        ctx = ctx['inner']
        self.assertEquals(ctx, expected)

        trace_logs = [log_locs[1] for log_locs in
                      logs['remote'].values()]
        # Maps walk_id->list of stuff
        walks = {}
        for trace_log in trace_logs:
            with open(trace_log, 'r') as f:
                for line in f:
                    record = encode.decode(line)
                    walk_id = record['walk_id']
                    current_walk = record['walk']
                    sync_point = record['sync_point']

                    if walk_id not in walks:
                        walks[walk_id] = []
                    walks[walk_id].append((current_walk, sync_point))
        # Checking my assumptions
        self.assertEquals(len(walks), 1)

        walk_to_replay = walk.Walk()
        for segment in walks.values()[0]:
            walk_segment = segment[0]
            sync_point = segment[1]
            if sync_point:
                walk_to_replay.append(sync_point)
            walk_to_replay += walk_segment

        # Replay walk directly
        ctx = {}
        runner.replay_multistage_walk(walk_to_replay, ctx=ctx)
        self.assertTrue('inner' in ctx)
        self.assertEquals(ctx['inner'], expected)

        # Now let's test the replay lib
        ctx = {}
        ctx = replay.replay_walk_by_id(logs['master'], walk_id, step=False,
                ctx=ctx)
        self.assertTrue('inner' in ctx)
        self.assertEquals(ctx['inner'], expected)

        #shutil.rmtree(log_dir)

if __name__ == "__main__":
    unittest.main()
