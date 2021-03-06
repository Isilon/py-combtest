from combtest.action import Action, SerialAction, OptionSet
from combtest.walk import WalkOptions

class Action1(Action):
    def run(self, item, state):
        state['l'].append(item)

    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

class Action2(Action1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1, 2)
        return OptionSet(options, action_class=cls)

class SerialAction1(SerialAction):
    def run(self, item, state):
        state['l'].append(item)
        return state

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

class SerialAction2(SerialAction1):
    @classmethod
    def get_option_set(cls):
        options = (0, 1)
        option_instances = [cls(option) for option in options]
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
        SerialAction1,
        Action3,
        Action4,
        SerialAction2,
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
assert s1._serial_action is None
s2 = s1._children[0]
assert isinstance(s2._serial_action, SerialAction1)
assert len(s2._children) == 2
# (half went down the other child of s1)
assert s2._walk_count == (972 / 2)
s3 = s2._children[0]
assert isinstance(s3._options[0][0], Action5)
options = [option.param for option in s3._options[0]]
assert options == list(range(3))

print("Try running the walks")
state = []
for _ in range(972):
    state.append({'l': []})
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

        if epoch.serial_action is not None:
            for idx in idxs:
                epoch.serial_action(state[idx])

        for i, walk in enumerate(walks):
            idx = idxs[i]
            max_walk_id = max(max_walk_id, idx)

            walk.execute(state[idx])

            if idx not in execution_count:
                execution_count[idx] = 1
            else:
                execution_count[idx] += 1

for walk_idx, branch_id in branch_ids.items():
    current_state = state[walk_idx]['l']
    assert len(branch_id) == 2
    assert current_state[2] == branch_id[0]
    assert current_state[5] == branch_id[1]

state_result = [list(r.values())[0] for r in state]
state_result.sort()


print("Compare to expected")
state_expected = []
for a1 in range(3):
    for a2 in range(3):
        for s1 in range(2):
            for a3 in range(3):
                for a4 in range(3):
                    for s2 in range(2):
                        for a5 in range(3):
                            state_inner = []
                            state_expected.append(state_inner)
                            state_inner.append(a1)
                            state_inner.append(a2)
                            state_inner.append(s1)
                            state_inner.append(a3)
                            state_inner.append(a4)
                            state_inner.append(s2)
                            state_inner.append(a5)

assert state_result == state_expected, \
        "\n%s\n%s" % (str(state_result), str(state_expected))

print("----")
print("Sync points at the start and end:")
# 2 ** 2 * 3 ** 3 = 108
walk_order2 = (SerialAction1,
               Action1,
               Action2,
               Action3,
               SerialAction2)
wo2 = WalkOptions(walk_order2)
t = wo2.tree
print(wo2)
assert len(t) == 2
s1 = t[0]
assert s1._walk_count == (108 / 2)
assert s1._serial_action is not None
assert len(s1._options) == 3
for option_set in s1._options:
    assert len(option_set) == 3
assert len(s1._children) == 2
s2 = s1._children[0]
assert not s2._options
assert not s2._children
assert s2._serial_action is not None

print("Try running the walks")
state = []
for _ in range(108):
    state.append({'l': []})

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

        if epoch.serial_action is not None:
            for idx in idxs:
                epoch.serial_action(state[idx])

        for i, walk in enumerate(walks):
            idx = idxs[i]
            max_walk_id = max(max_walk_id, idx)

            walk.execute(state[idx])

            if idx not in execution_count:
                execution_count[idx] = 1
            else:
                execution_count[idx] += 1

for walk_idx, branch_id in branch_ids.items():
    current_state = state[walk_idx]['l']
    assert len(branch_id) == 2
    assert current_state[0] == branch_id[0]
    assert current_state[4] == branch_id[1]

state_result = [list(r.values())[0] for r in state]
state_result.sort()

print("Compare to expected")
state_expected = []
for s1 in range(2):
    for a1 in range(3):
        for a2 in range(3):
            for a3 in range(3):
                for s2 in range(2):
                    state_inner = []
                    state_expected.append(state_inner)
                    state_inner.append(s1)
                    state_inner.append(a1)
                    state_inner.append(a2)
                    state_inner.append(a3)
                    state_inner.append(s2)
                    idx += 1
assert state_result == state_expected

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
assert s1._serial_action is None

print("Try running the walks")
state = []
for _ in range(81):
    state.append({'l': []})
for epoch_list in wo3:
    for epoch in epoch_list:
        if epoch.serial_action is not None:
            epoch.serial_action(state)
        for idx, branch_id, walk in epoch:
            assert not branch_id
            walk.execute(state[idx])

state_result = [list(r.values())[0] for r in state]

print("Compare to expected")
state_expected = []
for a1 in range(3):
    for a2 in range(3):
        for a3 in range(3):
            for a4 in range(3):
                state_inner = []
                state_expected.append(state_inner)
                state_inner.append(a1)
                state_inner.append(a2)
                state_inner.append(a3)
                state_inner.append(a4)
                idx += 1
assert state_result == state_expected
