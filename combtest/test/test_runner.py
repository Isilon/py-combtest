"""
test_replay_multistage_walk
test_run_multistage_walks
* include all options
"""
import os
import shutil
import tempfile
import unittest

from combtest.action import OptionSet, SyncPoint
import combtest.bootstrap as bootstrap
import combtest.encode as encode
import combtest.runner as runner
import combtest.walk as walk
from combtest.utils import get_my_IP

import combtest.test.classes.actions as actions

# Clean up traces and logs for debugging if True, otherwise preserve them, e.g.
# for debugging purposes.
CLEANUP_ON_EXIT = True

class TestRunner(unittest.TestCase):
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

    def test_single_stage_dispatch(self):
        os1 = actions.ActionAppend1.get_option_set()
        os2 = actions.ActionAppend2.get_option_set()
        os3 = actions.ActionAppend3.get_option_set()

        cnt1 = len(actions.ActionAppend1.OPTION_SET)
        cnt2 = len(actions.ActionAppend2.OPTION_SET)
        cnt3 = len(actions.ActionAppend3.OPTION_SET)

        expected_cnt = cnt1 * cnt2 * cnt3

        total_count, error_count, elapsed, ctxs = \
                runner.run_walks([os1, os2, os3],
                                 gather_ctxs=True,
                                 verbose=True)

        self.assertEqual(total_count, expected_cnt)
        self.assertEqual(error_count, 0)

        all_ctxs = []
        for worker_ctxs in ctxs:
            all_ctxs.extend([inner['inner'] for inner in worker_ctxs])
        all_ctxs.sort()

        current_idx = 0
        for i in range(cnt1):
            for j in range(cnt2):
                for k in range(cnt3):
                    expected = [i, j, k]
                    self.assertEqual(expected, all_ctxs[current_idx])
                    current_idx += 1

    def test_multistage(self):
        cnt1 = len(actions.ActionAppend1.OPTION_SET)
        cnt2 = len(actions.ActionAppend2.OPTION_SET)
        cnt3 = len(actions.ActionAppend3.OPTION_SET)

        sp_cnt1 = len(actions.SyncPointAppend1.OPTION_SET)
        sp_cnt2 = len(actions.SyncPointAppend2.OPTION_SET)

        expected_cnt = cnt1 * cnt2 * sp_cnt1 * cnt3 * sp_cnt2

        action_classes = [actions.ActionAppend1,
                          actions.ActionAppend2,
                          actions.SyncPointAppend1,
                          actions.ActionAppend3,
                          actions.SyncPointAppend2,
                         ]

        log_dir = self._get_temp_dir()

        walk_count, error_count, segment_count, elapsed, ctxs, _ = \
                runner.run_multistage_walks(action_classes,
                                            gather_ctxs=True,
                                            verbose=True,
                                            # (assumes we are running
                                            # everything locally)
                                            log_dir=log_dir,
                                           )

        self.assertEqual(walk_count, expected_cnt)
        # 3 below from: segment before sp1, segment between sp1 and sp2, and
        # after sp2.
        self.assertEqual(segment_count, expected_cnt * 3)
        self.assertEqual(error_count, 0)

        all_ctxs = []

        for worker_ctxs in ctxs:
            for walk_id, walk_ctx in worker_ctxs.items():
                inner = walk_ctx['inner']
                inner.append(walk_ctx['sp_value'])
                all_ctxs.append(inner)
        all_ctxs.sort()

        current_idx = 0
        for i in range(cnt1):
            for j in range(cnt2):
                for k in range(sp_cnt1):
                    for l in range(cnt3):
                        for m in range(sp_cnt2):
                            expected = [i, j, k, l, m]
                            self.assertEqual(expected, all_ctxs[current_idx])
                            current_idx += 1


    def test_multistage_leading_syncpoint(self):
        cnt1 = len(actions.ActionAppend1.OPTION_SET)
        cnt2 = len(actions.ActionAppend2.OPTION_SET)

        sp_cnt1 = len(actions.SyncPointAppend1.OPTION_SET)
        sp_cnt2 = len(actions.SyncPointAppend2.OPTION_SET)

        expected_cnt = sp_cnt1 * cnt1 * cnt2 * sp_cnt2

        log_dir = self._get_temp_dir()

        walk_count, error_count, segment_count, elapsed, ctxs, _ = \
                runner.run_multistage_walks([
                                            actions.SyncPointAppend1,
                                            actions.ActionAppend1,
                                            actions.ActionAppend2,
                                            actions.SyncPointAppend2,
                                            ],
                                 gather_ctxs=True,
                                 log_dir=log_dir,
                                 verbose=True)

        self.assertEqual(walk_count, expected_cnt)
        # 2 below from: segment between sp1 and sp2, and
        # after sp2.
        self.assertEqual(segment_count, expected_cnt * 2)
        self.assertEqual(error_count, 0)

        all_ctxs = []

        for worker_ctxs in ctxs:
            for walk_id, walk_ctx in worker_ctxs.items():
                inner = walk_ctx['inner']
                inner.append(walk_ctx['sp_value'])
                all_ctxs.append(inner)
        all_ctxs.sort()

        current_idx = 0
        for i in range(sp_cnt1):
            for j in range(cnt1):
                for k in range(cnt2):
                    for l in range(sp_cnt2):
                        expected = [i, j, k, l]
                        self.assertEqual(expected, all_ctxs[current_idx])
                        current_idx += 1


if __name__ == "__main__":
    unittest.main()
