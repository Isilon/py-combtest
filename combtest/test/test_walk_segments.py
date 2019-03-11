from combtest.action import Action, SyncPoint, OptionSet
from combtest.walk import WalkOptions

class Action1(Action):
    def run(self, item, dynamic_ctx):
        dynamic_ctx['l'].append(item)

    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

class Action2(Action1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

class SyncPoint1(SyncPoint):
    def run(self, item, dynamic_ctx):
        dynamic_ctx['l'].append(item)

    @classmethod
    def get_option_set(cls):
        options = (0, 1)
        option_instances = [cls(option) for option in options]
        option_instances[0].is_nop = True
        return iter(option_instances)

class Action3(Action1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

class Action4(Action1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

class SyncPoint2(SyncPoint1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1)
        option_instances = [cls(option) for option in options]
        option_instances[0].is_nop = True
        return iter(option_instances)

class Action5(Action1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

# 3 ** 5 * 2 ** 2 = 972 walks
# They should look like:
# 0, 0, 0, 0, 0, 0, 0
# 1, 0, 0, 0, 0, 0, 0
# 2, 0, 0, 0, 0, 0, 0
# 0, 1, 0, 0, 0, 0, 0
# ...
# (order actually doesn't matter, so long as we generate 972 unique walks)
walk_order = (
        Action1,
        Action2,
        SyncPoint1,
        Action3,
        Action4,
        SyncPoint2,
        Action5,
        )
wo = WalkOptions(walk_order)
t = wo.tree

print("Sync points in the middle")
print("Tree is:")
print(wo)

assert len(t) == 1
s1 = t[0]
assert s1._walk_count == 972
assert len(s1._children) == 2
assert s1._sync_point is None
s2 = s1._children[0]
assert isinstance(s2._sync_point, SyncPoint1)
assert len(s2._children) == 2
# (half went down the other child of s1)
assert s2._walk_count == (972 / 2)
s3 = s2._children[0]
assert isinstance(s3._options[0][0], Action5)
options = [option.static_ctx for option in s3._options[0]]
assert options == list(range(3))

print("Try running the walks")
ctx = []
for _ in range(972):
    ctx.append({'l': []})
max_walk_id = -1
execution_count = {}

# Map walk_idx->branch_id
branch_ids = {}
for epoch_list in wo:
    for epoch in epoch_list:
        walks = []
        idxs = []
        for idx, branch_id, walk in epoch:
            walks.append(walk)
            idxs.append(idx)

            # Test that branch_id is working
            if idx in branch_ids:
                assert branch_ids[idx] == branch_id
            else:
                branch_ids[idx] = branch_id

        if epoch.sync_point is not None:
            for idx in idxs:
                epoch.sync_point(ctx[idx])

        for i, walk in enumerate(walks):
            idx = idxs[i]
            max_walk_id = max(max_walk_id, idx)

            walk.execute(ctx[idx])

            if idx not in execution_count:
                execution_count[idx] = 1
            else:
                execution_count[idx] += 1

for walk_idx, branch_id in branch_ids.items():
    current_ctx = ctx[walk_idx]['l']
    assert len(branch_id) == 2
    assert current_ctx[2] == branch_id[0]
    assert current_ctx[5] == branch_id[1]

ctx_result = [list(r.values())[0] for r in ctx]
ctx_result.sort()


print("Compare to expected")
ctx_expected = []
for a1 in range(3):
    for a2 in range(3):
        for s1 in range(2):
            for a3 in range(3):
                for a4 in range(3):
                    for s2 in range(2):
                        for a5 in range(3):
                            ctx_inner = []
                            ctx_expected.append(ctx_inner)
                            ctx_inner.append(a1)
                            ctx_inner.append(a2)
                            ctx_inner.append(s1)
                            ctx_inner.append(a3)
                            ctx_inner.append(a4)
                            ctx_inner.append(s2)
                            ctx_inner.append(a5)

assert ctx_result == ctx_expected, \
        "\n%s\n%s" % (str(ctx_result), str(ctx_expected))

print("----")
print("Sync points at the start and end:")
# 2 ** 2 * 3 ** 3 = 108
walk_order2 = (SyncPoint1,
               Action1,
               Action2,
               Action3,
               SyncPoint2)
wo2 = WalkOptions(walk_order2)
t = wo2.tree
print(wo2)
assert len(t) == 2
s1 = t[0]
assert s1._walk_count == (108 / 2)
assert s1._sync_point is not None
assert len(s1._options) == 3
for option_set in s1._options:
    assert len(option_set) == 3
assert len(s1._children) == 2
s2 = s1._children[0]
assert not s2._options
assert not s2._children
assert s2._sync_point is not None

print("Try running the walks")
ctx = []
for _ in range(108):
    ctx.append({'l': []})

# Map walk_idx->branch_id
branch_ids = {}
for epoch_list in wo2:
    for epoch in epoch_list:
        walks = []
        idxs = []
        for idx, branch_id, walk in epoch:
            walks.append(walk)
            idxs.append(idx)

            # Test that branch_id is working
            if idx in branch_ids:
                assert branch_ids[idx] == branch_id
            else:
                branch_ids[idx] = branch_id

        if epoch.sync_point is not None:
            for idx in idxs:
                epoch.sync_point(ctx[idx])

        for i, walk in enumerate(walks):
            idx = idxs[i]
            max_walk_id = max(max_walk_id, idx)

            walk.execute(ctx[idx])

            if idx not in execution_count:
                execution_count[idx] = 1
            else:
                execution_count[idx] += 1

for walk_idx, branch_id in branch_ids.items():
    current_ctx = ctx[walk_idx]['l']
    assert len(branch_id) == 2
    assert current_ctx[0] == branch_id[0]
    assert current_ctx[4] == branch_id[1]

ctx_result = [list(r.values())[0] for r in ctx]
ctx_result.sort()

print("Compare to expected")
ctx_expected = []
for s1 in range(2):
    for a1 in range(3):
        for a2 in range(3):
            for a3 in range(3):
                for s2 in range(2):
                    ctx_inner = []
                    ctx_expected.append(ctx_inner)
                    ctx_inner.append(s1)
                    ctx_inner.append(a1)
                    ctx_inner.append(a2)
                    ctx_inner.append(a3)
                    ctx_inner.append(s2)
                    idx += 1
assert ctx_result == ctx_expected

print("----")
print("No sync point case")
# 3 ** 4 = 81
walk_order3 = (Action1,
               Action2,
               Action3,
               Action4
              )
wo3 = WalkOptions(walk_order3)
t = wo3.tree
print(wo3)
assert len(t) == 1
s1 = t[0]
assert s1._walk_count == 81
assert len(s1._children) == 0
assert s1._sync_point is None

print("Try running the walks")
ctx = []
for _ in range(81):
    ctx.append({'l': []})
for epoch_list in wo3:
    for epoch in epoch_list:
        if epoch.sync_point is not None:
            epoch.sync_point(ctx)
        for idx, branch_id, walk in epoch:
            assert not branch_id
            walk.execute(ctx[idx])

ctx_result = [list(r.values())[0] for r in ctx]

print("Compare to expected")
ctx_expected = []
for a1 in range(3):
    for a2 in range(3):
        for a3 in range(3):
            for a4 in range(3):
                ctx_inner = []
                ctx_expected.append(ctx_inner)
                ctx_inner.append(a1)
                ctx_inner.append(a2)
                ctx_inner.append(a3)
                ctx_inner.append(a4)
                idx += 1
assert ctx_result == ctx_expected
