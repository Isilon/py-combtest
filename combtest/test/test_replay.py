"""
Test walk replay mechanisms
"""
import os
import random
import shutil
import tempfile
import unittest


from combtest.action import OptionSet, SerialAction
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

class TestReplayByLib(unittest.TestCase):
    """
    This is pretty much just a local 'run walk' function. Let's get code
    coverage of it anyway.
    """

    def _get_random_walk(self):
        s1 = random_option(actions.SerialActionAppend1.get_option_set())
        a1 = random_option(actions.ActionAppend1.get_option_set())
        a2 = random_option(actions.ActionAppend1.get_option_set())

        test_walk = walk.Walk([s1, a1, a2])
        return test_walk

    def test_walk_replay(self):
        test_walk = self._get_random_walk()

        state = {}
        canceled = runner.replay_walk(test_walk, state=state)
        self.assertFalse(canceled)


        result = state['inner']
        expected = [a.param for a in test_walk]
        self.assertEqual(result, expected)

class TestReplayFromTrace(unittest.TestCase):
    def test_walk_replay(self):
        log_dir = tempfile.mkdtemp()

        walk_order = [actions.ActionSingleton1,
                      actions.ActionSingleton2,
                      actions.SerialActionSingleton1,
                      actions.ActionSingleton3]
        # See classes.py
        expected = [1, 2, 0, 3]

        state = {}
        walk_count, error_count, _, _, states, logs = \
                runner.run_tests(walk_order,
                                 verbose=2,
                                 log_dir=log_dir,
                                 state=state,
                                 gather_states=True)
        self.assertEqual(walk_count, 1)
        self.assertEqual(error_count, 0)

        # Single worker
        self.assertEqual(len(states), 1)
        # Single walk on that worker
        worker_states = states[0]
        self.assertEqual(len(worker_states), 1)
        state = worker_states[0]
        state = state['inner']
        self.assertEqual(state, expected)

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
        self.assertEqual(len(walks), 1)

        walk_to_replay = walk.Walk()
        for segment in list(walks.values())[0]:
            walk_segment = segment[0]
            sync_point = segment[1]
            if sync_point:
                walk_to_replay.append(sync_point)
            walk_to_replay += walk_segment

        # Replay walk directly
        state = {}
        runner.replay_walk(walk_to_replay, state=state)
        self.assertTrue('inner' in state)
        self.assertEqual(state['inner'], expected)

        # Now let's test the replay lib
        state = {}
        state = replay.replay_walk_by_id(logs['master'], walk_id, step=False,
                                         state=state)
        self.assertTrue('inner' in state)
        self.assertEqual(state['inner'], expected)

        shutil.rmtree(log_dir)

if __name__ == "__main__":
    unittest.main()
