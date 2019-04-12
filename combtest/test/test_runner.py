import os
import shutil
import tempfile
import unittest

from combtest.action import OptionSet, SerialAction, Action
import combtest.bootstrap as bootstrap
import combtest.encode as encode
import combtest.runner as runner
import combtest.walk as walk
from combtest.utils import get_my_IP

import combtest.test.classes.actions as actions

# Clean up traces and logs for debugging if True, otherwise preserve them, e.g.
# for debugging purposes.
CLEANUP_ON_EXIT = True

class _TestBase(unittest.TestCase):
    def tearDown(self):
        if CLEANUP_ON_EXIT:
            try:
                for temp_dir in self.temp_dirs:
                    try:
                        shutil.rmtree(temp_dir)
                    except OSError:
                        pass
            except (AttributeError, TypeError):
                pass

            self.temp_dir = []

    def _get_temp_dir(self):
        log_dir = tempfile.mkdtemp(dir=os.curdir)

        try:
            self.temp_dirs.append(log_dir)
        except AttributeError:
            self.temp_dirs = []
            self.temp_dirs.append(log_dir)

        return log_dir

class TestRunner(_TestBase):

    def test_no_state_provided(self):
        runner.run_tests([actions.ActionExpectNoState],
                         gather_states=True)

        runner.run_tests([actions.ActionExpectNoState],
                         gather_states=False)

    def test_single_stage_dispatch(self):
        os1 = actions.ActionAppend1.get_option_set()
        os2 = actions.ActionAppend2.get_option_set()
        os3 = actions.ActionAppend3.get_option_set()

        cnt1 = len(actions.ActionAppend1.OPTION_SET)
        cnt2 = len(actions.ActionAppend2.OPTION_SET)
        cnt3 = len(actions.ActionAppend3.OPTION_SET)

        expected_cnt = cnt1 * cnt2 * cnt3

        total_count, error_count, _, elapsed, states, _ = \
                runner.run_tests([actions.ActionAppend1,
                                  actions.ActionAppend2,
                                  actions.ActionAppend3],
                                 gather_states=True,
                                 state={},
                                 verbose=True)

        self.assertEqual(total_count, expected_cnt)
        self.assertEqual(error_count, 0)

        all_states = []
        for worker_states in states:
            all_states.extend([inner['inner'] for inner in
                               worker_states])
        all_states.sort()

        current_idx = 0
        for i in range(cnt1):
            for j in range(cnt2):
                for k in range(cnt3):
                    expected = [i, j, k]
                    self.assertEqual(expected, all_states[current_idx])
                    current_idx += 1

    def test_multistage(self):
        cnt1 = len(actions.ActionAppend1.OPTION_SET)
        cnt2 = len(actions.ActionAppend2.OPTION_SET)
        cnt3 = len(actions.ActionAppend3.OPTION_SET)

        sp_cnt1 = len(actions.SerialActionAppend1.OPTION_SET)
        sp_cnt2 = len(actions.SerialActionAppend2.OPTION_SET)

        expected_cnt = cnt1 * cnt2 * sp_cnt1 * cnt3 * sp_cnt2

        action_classes = [actions.ActionAppend1,
                          actions.ActionAppend2,
                          actions.SerialActionAppend1,
                          actions.ActionAppend3,
                          actions.SerialActionAppend2,
                         ]

        log_dir = self._get_temp_dir()

        walk_count, error_count, segment_count, elapsed, states, _ = \
                runner.run_tests(action_classes,
                                 gather_states=True,
                                 verbose=True,
                                 state={},
                                 # (assumes we are running
                                 # everything locally)
                                 log_dir=log_dir,
                                )

        self.assertEqual(walk_count, expected_cnt)
        # 3 below from: segment before sp1, segment between sp1 and sp2, and
        # after sp2.
        self.assertEqual(segment_count, expected_cnt * 3)
        self.assertEqual(error_count, 0)

        all_states = []

        for worker_states in states:
            for walk_state in worker_states:
                inner = walk_state['inner']
                inner.append(walk_state['sp_value'])
                all_states.append(inner)
        all_states.sort()

        current_idx = 0
        for i in range(cnt1):
            for j in range(cnt2):
                for k in range(sp_cnt1):
                    for l in range(cnt3):
                        for m in range(sp_cnt2):
                            expected = [i, j, k, l, m]
                            self.assertEqual(expected, all_states[current_idx])
                            current_idx += 1


    def test_multistage_leading_syncpoint(self):
        cnt1 = len(actions.ActionAppend1.OPTION_SET)
        cnt2 = len(actions.ActionAppend2.OPTION_SET)

        sp_cnt1 = len(actions.SerialActionAppend1.OPTION_SET)
        sp_cnt2 = len(actions.SerialActionAppend2.OPTION_SET)

        expected_cnt = sp_cnt1 * cnt1 * cnt2 * sp_cnt2

        log_dir = self._get_temp_dir()

        walk_count, error_count, segment_count, elapsed, states, _ = \
                runner.run_tests([
                                  actions.SerialActionAppend1,
                                  actions.ActionAppend1,
                                  actions.ActionAppend2,
                                  actions.SerialActionAppend2,
                                 ],
                                 gather_states=True,
                                 log_dir=log_dir,
                                 verbose=True,
                                 state={},
                                )

        self.assertEqual(walk_count, expected_cnt)
        # 2 below from: segment between sp1 and sp2, and
        # after sp2.
        self.assertEqual(segment_count, expected_cnt * 2)
        self.assertEqual(error_count, 0)

        all_states = []

        for worker_states in states:
            for walk_state in worker_states:
                inner = walk_state['inner']
                inner.append(walk_state['sp_value'])
                all_states.append(inner)
        all_states.sort()

        current_idx = 0
        for i in range(sp_cnt1):
            for j in range(cnt1):
                for k in range(cnt2):
                    for l in range(sp_cnt2):
                        expected = [i, j, k, l]
                        self.assertEqual(expected, all_states[current_idx])
                        current_idx += 1

#class TestCustomState(_TestBase):
#    def test_remote_state_update(self):
#        #TODO: update w/ something bool(True)
#        #TODO: update w/ something bool(False)

class TestSerialActionNegativeCases(_TestBase):
    def test_no_remote_update_supported(self):

        log_dir = self._get_temp_dir()

        # attempt to update the remote state (not supported, since it
        # is a list). Should get an error back
        action_classes = [actions.ActionAppendList1,
                          actions.SerialActionTryUpdate,
                          actions.ActionAppendList2]
        state = []
        self.assertRaises(RuntimeError,
                          runner.run_tests,
                          action_classes,
                          gather_states=True,
                          verbose=True,
                          state=state,
                          log_dir=log_dir,
                         )

    def test_dont_try(self):
        log_dir = self._get_temp_dir()
        action_classes = [actions.ActionAppendList1,
                          actions.SerialActionDontTryUpdate,
                          actions.ActionAppendList2]
        state = []
        walk_count, error_count, segment_count, elapsed, states, _ = \
                runner.run_tests(action_classes,
                                 gather_states=True,
                                 log_dir=log_dir,
                                 verbose=2,
                                 state=state,
                                )

        expected_cnt = (len(actions.ActionAppendList1.OPTIONS) *
                        len(actions.SerialActionDontTryUpdate.OPTIONS) *
                        len(actions.ActionAppendList2.OPTIONS))
        self.assertEqual(walk_count, expected_cnt)
        self.assertEqual(error_count, 0)

        # Split above/below the sync point
        self.assertEqual(segment_count, expected_cnt * 2)

        all_states = []
        for worker_states in states:
            for walk_state in worker_states:
                all_states.append(walk_state)
        all_states.sort()

        current_idx = 0
        for opt1 in actions.ActionAppendList1.OPTIONS:
            for opt2 in actions.ActionAppendList2.OPTIONS:
                expected = [opt1, opt2]
                for _ in range(len(actions.SerialActionDontTryUpdate.OPTIONS)):
                    self.assertEqual(expected, all_states[current_idx])
                    current_idx += 1


if __name__ == "__main__":
    unittest.main()
