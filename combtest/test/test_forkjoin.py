import unittest

import combtest.forkjoin as forkjoin


def my_inner_func(idx, fail=False):
    if fail:
        raise RuntimeError(str(idx))
    return idx

class TestForkjoin(unittest.TestCase):
    def test_all_pass(self):
        count = 100

        work = []
        for idx in range(count):
            wi = forkjoin.WorkItem(my_inner_func, idx)
            work.append(wi)

        results = forkjoin.fork_join(work)
        self.assertEqual(results, list(range(100)))

    def test_some_fail(self):
        count = 100

        work = []
        for idx in range(count):
            if idx in (1, 50, 99):
                wi = forkjoin.WorkItem(my_inner_func, idx, fail=True)
            else:
                wi = forkjoin.WorkItem(my_inner_func, idx)
            work.append(wi)

        results = forkjoin.fork_join(work, suppress_errors=True)

        for idx in range(count):
            if idx in (1, 50, 99):
                self.assertTrue(isinstance(results[idx], RuntimeError))
            else:
                self.assertEqual(results[idx], idx)

if __name__ == '__main__':
    unittest.main()
