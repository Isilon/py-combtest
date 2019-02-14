"""
This module provides ways of systematically generating operations and
combinations thereof. This should be abstract enough to be applicable to many
different domains. An overview of the moving parts:

    Walk: a list of Actions, and the context in which an Action is executed.
          Example: set protection of the file, select some range within it,
          write over that range, take a snapshot.
    StateCombinator: Generates Walks based on the cartesian product of 2 or
                     more OptionSets.
"""
import copy
from collections import Iterable
import importlib
import itertools
import json
import logging
import operator
import threading
import traceback


from combtest.action import SyncPoint, Action
import combtest.central_logger as central_logger
from combtest.central_logger import logger
import combtest.encode as encode
from combtest.utils import RangeTree, get_class_qualname, \
                           get_class_from_qualname


class CancelWalk(RuntimeError):
    pass

class WalkFailedError(RuntimeError):
    pass



class Walk(object):
    """
    A Walk, named after the graph theory concept, is a list of Actions to
    execute. The notion here is that each action takes us through a transition
    in state/action space.
    """
    def __init__(self, *elems):
        _elems = []
        if len(elems) == 1 and isinstance(elems, Iterable):
            for elem in elems[0]:
                _elems.append(elem)
        else:
            _elems = []
            for elem in elems:
                if not isinstance(elem, Action):
                    raise ValueError("Walks must contain only Actions; "
                            "got: %s" % str(type(elem)))
                _elems.append(elem)

        self._elems = _elems

    def append(self, elem):
        self._elems.append(elem)

    def __len__(self):
        return len(self._elems)

    def __eq__(self, other):
        if not isinstance(other, Walk):
            return False
        return other._elems == self._elems

    def __add__(self, other):
        if not isinstance(other, Walk):
            raise TypeError("Cannot add a Walk with object of type %s" %
                    str(type(other)))
        elems = self._elems + other._elems
        return self.__class__(*elems)

    def __iter__(self):
        return iter(self._elems)

    def execute(self, dynamic_ctx, log_errors=True):
        """
        Execute the ops in order.
        Returns True if the Walk was run successfully, False otherwise.
        If an Action raises CancelWalk, we will stop executing immediately and
        return True.
        """
        try:
            for op in self:
                # I will leave it up to the user to decide to log or not, and
                # the appropriate verbosity.
                op(ctx=dynamic_ctx)
        except CancelWalk as e:
            # Likewise here: let the user decide to log or not in their layer
            return True
        except Exception as e:
            # PORT/WRAP: no dynamic_ctx specifics here.
            msg_ctx = "Walk was: %s\nctx: %s" % (repr(self), repr(dynamic_ctx))
            exc = type(e)(str(e) + "\n" + msg_ctx)
            if log_errors:
                logger.exception(exc)
                logger.error(msg_ctx)

            new_msg = "Walk failed:\n"
            new_msg += traceback.format_exc()
            new_msg += "\n" + msg_ctx
            wfe = WalkFailedError(new_msg)
            if hasattr(e, 'errno'):
                wfe.errno = e.errno
            raise wfe

        return False

    def __repr__(self):
        return self.as_json()

    def as_json(self):
        encoded = encode.encode(self)
        return encoded

    def to_json(self):
        return list(self._elems)

    @classmethod
    def from_json(cls, obj):
        try:
            assert not isinstance(obj, basestring), str(obj)
            out = cls()
            for act in obj:
                out.append(act)
        except TypeError:
            raise ValueError("Not recognized as an encoded walk: %s" %
                    str(json_str))

        return out


class Segment(object):
    # Presume not threadsafe

    def __init__(self, walk_count, options=(),
            sync_point_instance=None,
            parent_period=1,
            level=0):

        assert options or sync_point_instance
        assert sync_point_instance is None or isinstance(sync_point_instance,
                SyncPoint)

        self._walk_count = walk_count
        self._count_walks_produced = 0
        self._level = level

        # Run *after* the SyncPoint, if there is one. That means that if this
        # Segment represents the leaf of the tree and we have a SyncPoint,
        # then options is None.
        # tuple([option1_class1, option2_class1, ...],
        #       [option1_class2, option2_class2, ...],
        #       ...
        #      )
        self._options = tuple(options)
        self._current_repeat_count = 0
        self._held_walk = None
        self._parent_period = parent_period

        # Run *before* the segment; thus it is None if e.g. we have no
        # SyncPoints at all.
        self._sync_point = sync_point_instance

        # Created after initialization because of our construction method
        # below.
        self._children = []

        # Used to produce segments of walks
        self._state_combinator = None
        self._refresh_state_combinator()

    def _refresh_state_combinator(self):
        self._state_combinator = StateCombinator(*self._options)

    def add_child(self, child):
        self._children.append(child)

    @property
    def children(self):
        return copy.copy(self._children)

    @property
    def sync_point(self):
        return self._sync_point

    @property
    def walk_count(self):
        return self._walk_count

    @property
    def level(self):
        return self._level

    def reset(self):
        self._count_walks_produced = 0
        self._current_repeat_count = 0
        self._held_walk = None
        self._refresh_state_combinator()

    def __iter__(self):
        return self

    def next(self):
        if self._count_walks_produced == self._walk_count:
            raise StopIteration()

        if self._held_walk is None:
            self._held_walk = self._state_combinator.next()

        walk = self._held_walk
        self._count_walks_produced += 1
        self._current_repeat_count += 1

        if self._current_repeat_count == self._parent_period:
            self._current_repeat_count = 0

            if self._count_walks_produced == self._walk_count:
                check = True
            else:
                check = False

            try:
                self._held_walk = self._state_combinator.next()
            except StopIteration:
                self._refresh_state_combinator()
                self._held_walk = self._state_combinator.next()
            else:
                if check:
                    raise RuntimeError("Should have exhausted iterator")

        return walk

class Epoch(object):
    # Not threadsafe

    def __init__(self, walk_idx_start, walk_idx_end, range_tree,
            sync_point=None, walks=None, child=None, level=0):
        """
        Represents a set of stuff (e.g. walk segments) that can run
        in parallel to other such sets of stuff. The sync point will be
        executed before the walks. 'child' points to a segment we can run
        once this Epoch finishes.
        """
        self.sync_point = sync_point
        self.walks = tuple(walks)
        self.child = child
        self.walk_idx_start = walk_idx_start
        self.walk_idx_end = walk_idx_end
        self.range_tree = range_tree

        self._level = level

        # This means we calculate once here and once in next(), which is 2 *
        # O(N). I've gone this route instead of saving a map of
        # walk_idx->branch_id since that would be O(N) in mem, which is worse
        # in my opinion. This is all cheap in-memory operation.
        self.branch_ids = set([self.range_tree.provide(idx) for idx in
                range(walk_idx_start, walk_idx_end)])

        self._current_walk_idx = self.walk_idx_start

    @property
    def level(self):
        return self._level

    def __iter__(self):
        return self

    def next(self):
        if self._current_walk_idx == self.walk_idx_end:
            raise StopIteration()

        idx = self._current_walk_idx - self.walk_idx_start
        walk = self.walks[idx]

        walk_idx = self._current_walk_idx
        branch_id = self.range_tree.provide(walk_idx)
        self.branch_ids.add(branch_id)
        self._current_walk_idx += 1

        return walk_idx, branch_id, walk


class WalkOptions(object):
    # Not threadsafe

    def __init__(self, walk_order):
        self._sync_point_idxs = []
        self._sizes = []

        # Calculated on first use, cached here; safe since since we are
        # intended to be immutable.
        self._tree = None

        # Maps walk_id->branch_id
        self._branch_ids = []

        # [ [options1, options2, ...], syncpoint1, [options3, options4, ...], ...]
        # And each 'optionsX' is itself an iterable of options as you'd see
        # from SomeActionClass.get_option_set().
        self._segment_options = []

        seg_start_idx = 0
        # ('declare' here to cover the 'not walk_order' case for our tail
        #  flush below)
        action_class = None
        idx = 0
        for idx, action_class in enumerate(walk_order):
            if issubclass(action_class, SyncPoint):
                self._sync_point_idxs.append(idx)

                # Append any action-ish segments, and the sync point
                actions = walk_order[seg_start_idx:idx]
                if actions:
                    actions = tuple([tuple(ac.get_option_set()) for ac in
                            actions])
                    self._segment_options.append(actions)
                # else e.g. idx = 0: the first action_class is a SyncPoint
                self._segment_options.append(action_class)

                seg_start_idx = idx + 1

            option_set = tuple(action_class.get_option_set())
            self._sizes.append(len(option_set))

        # Tail flush, if we haven't already.
        if not issubclass(action_class, SyncPoint):
            actions = walk_order[seg_start_idx:]
            if actions:
                actions = tuple([tuple(ac.get_option_set()) for ac in
                        actions])
                self._segment_options.append(actions)

        self.walk_count = reduce(operator.mul, self._sizes)
        # During iteration, set to a list of:
        #  [(segment, start_idx, end_idx)]
        self._frontier = None

    @property
    def sizes(self):
        return tuple(self._sizes)

    def _get_next_options(self, segment_options, start_idx=0):
        walk_options = []
        idx = start_idx
        count = 0
        if idx < len(segment_options) and \
                isinstance(segment_options[start_idx], tuple):
            idx += 1
            current_options = segment_options[start_idx]
            walk_options.extend(current_options)
            count = reduce(operator.mul, [len(wo) for wo in current_options])

        return walk_options, idx, count

    def _ensure_tree_impl(self, walk_count, segment_options, start_idx=0,
            period=1, level=0):
        # Called recursively with SyncPoint at start, or this is the first
        # time it is called.

        # Base case
        if start_idx >= len(segment_options):
            assert start_idx == len(segment_options)
            return []

        # These become the children of the next level up, or the collection of
        # roots at the very top (which we expose in self.tree).
        root_segments = []

        if isinstance(segment_options[start_idx], tuple):
            walk_options, end_idx, walk_option_count = \
                    self._get_next_options(segment_options,
                    start_idx=start_idx)

            assert (walk_count % walk_option_count) == 0
            children = self._ensure_tree_impl(walk_count,
                    segment_options, start_idx=end_idx,
                    period=(period * walk_option_count),
                    level=(level + 1))

            new_segment = Segment(walk_count, options=walk_options,
                    parent_period=period,
                    level=level)
            for child in children:
                new_segment.add_child(child)

            root_segments.append(new_segment)
        else:
            current_sync_point = segment_options[start_idx]
            sync_points = [instance for instance in
                    current_sync_point.get_option_set()]
            sync_point_count = len(sync_points)

            assert (walk_count % sync_point_count) == 0
            segment_walk_count = walk_count / sync_point_count

            walk_options, end_idx, walk_option_count = \
                    self._get_next_options(segment_options,
                    start_idx=(start_idx + 1))
            assert walk_option_count == 0 or \
                    (segment_walk_count % walk_option_count) == 0

            children = self._ensure_tree_impl(segment_walk_count,
                    segment_options, start_idx=end_idx,
                    period=(period * walk_option_count),
                    level=(level + 1))

            for sync_point in sync_points:
                new_segment = Segment(segment_walk_count, options=walk_options,
                        sync_point_instance=sync_point, parent_period=period,
                        level=level)
                for child in children:
                    new_segment.add_child(child)
                root_segments.append(new_segment)

        return root_segments

    def _split_branch_ids(self, root, segments, start_idx, end_idx):
        current_start_idx = start_idx

        max_idxs = []
        values = []
        for segment in segments:
            current_end_idx = current_start_idx + segment.walk_count
            assert current_end_idx <= end_idx
            if segment.sync_point is not None:
                current_value = segment.sync_point.static_ctx
                values.append(current_value)
            else:
                values.append(None)
            max_idxs.append(current_end_idx)
            current_start_idx = current_end_idx

        children = root.split(max_idxs, values)

        current_start_idx = start_idx
        for idx, child in enumerate(children):
            segment = segments[idx]
            self._split_branch_ids(child, segment.children, current_start_idx,
                    child.max_idx)
            current_start_idx = child.max_idx

    def _refresh_branch_ids(self):
        branch_ids = RangeTree(0, self.walk_count)
        self._split_branch_ids(branch_ids, self._tree, 0, self.walk_count)
        self._branch_ids = branch_ids

    def _ensure_tree(self):
        if self._tree is None:
            # An iterable of segments that root a tree.
            self._tree = self._ensure_tree_impl(self.walk_count,
                    self._segment_options)
            self._refresh_branch_ids()

    @property
    def tree(self):
        self._ensure_tree()
        return self._tree

    def _print_tree(self, tree, depth=0):
        # DFS
        out = []
        prefix = "  " * depth + "-->"
        out.append(prefix + str(tree.walk_count))
        for child in tree.children:
            out.extend(self._print_tree(child, depth=(depth + 1)))
        return out

    def __str__(self):
        out = []
        for tree in self.tree:
            s = self._print_tree(tree)
            out.extend(s)

        return "\n".join(out)

    def __iter__(self):
        return self

    def _make_segment_epochs(self, walk_idx_start, walk_idx_end, segment):
        expected_walk_count = walk_idx_end - walk_idx_start

        segment.reset()
        walks = [walk for walk in segment]
        assert len(walks) == expected_walk_count, "%d %d" % (len(walks),
                expected_walk_count)

        epochs = []
        children = segment.children
        if children:
            assert (expected_walk_count % len(children)) == 0
            walks_per_child = expected_walk_count / len(children)

            current_walk_idx_start = walk_idx_start
            slice_idx = 0
            for child in children:
                current_walks = walks[slice_idx:(slice_idx + walks_per_child)]
                current_epoch = Epoch(current_walk_idx_start,
                        current_walk_idx_start + walks_per_child,
                        self._branch_ids,
                        sync_point=segment.sync_point,
                        walks=current_walks,
                        child=child,
                        level=segment.level)
                epochs.append(current_epoch)

                slice_idx += walks_per_child
                current_walk_idx_start += walks_per_child
        else:
            current_epoch = Epoch(walk_idx_start,
                    walk_idx_end,
                    self._branch_ids,
                    sync_point=segment.sync_point,
                    walks=walks,
                    child=None,
                    level=segment.level)
            epochs.append(current_epoch)

        return epochs

    def _ensure_frontier(self):
        if self._frontier is None:
            segment_count = len(self.tree)
            assert (self.walk_count % segment_count) == 0
            walks_per_segment = self.walk_count / segment_count
            walk_start_idx = 0
            frontier = []
            for segment in self.tree:
                current = (segment, walk_start_idx, walk_start_idx +
                        walks_per_segment)
                frontier.append(current)
                walk_start_idx += walks_per_segment
            self._frontier = frontier

    def _expand_temp_frontier(self, epochs, frontier):
        """
        Args:
            frontier - an iterable of Segment
        """
        # TODO
        return epochs, frontier

    def next(self):
        # Algorithm:
        # Epochs will start at each root: one for each second-level child
        #   Segment.
        # Once a given child's Epoch finishes, repeat for the tree it roots.
        # Repeat until we hit all leafs.
        self._ensure_frontier()
        if not self._frontier:
            raise StopIteration()

        new_frontier = []
        epochs = []
        while self._frontier:
            frontier_additions = []
            segment, walk_start_idx, walk_end_idx = self._frontier.pop(0)
            new_epochs = self._make_segment_epochs(walk_start_idx,
                    walk_end_idx, segment)

            for epoch in new_epochs:
                if epoch.child:
                    segment = epoch.child
                    start_idx = epoch.walk_idx_start
                    end_idx = epoch.walk_idx_end
                    frontier_additions.append((segment, start_idx, end_idx))

            # This step gives us an efficiency improvement that is likely to be
            # noticeable, so I want to address it in this initial design. Some
            # SyncPoint values are effectively no-ops, and in that case we
            # don't need to actually synchronize. So expand the corresponding
            # walks to go ahead and execute the next segment too.
            # Example: if we have a sync point with settings "take snapshot"
            # and "don't take snapshot". In the latter case there is no reason
            # to synchronize at all, so just extend the walks past the sync
            # point.
            new_epochs, frontier_additions = \
                    self._expand_temp_frontier(new_epochs, frontier_additions)

            epochs.extend(new_epochs)
            new_frontier.extend(frontier_additions)

        self._frontier = new_frontier

        return epochs


class StateCombinator(object):
    """
    A StateCombinator gives us all combinations of elements from the provided
    iterables. The idea is that we can systematically exhaust all walks of a
    given subset from the state/action space. For example: we could have one
    iterable providing mirroring vs fec protection for a file, another iterable
    setting different block types in the first cluster of the file, and a 3rd
    that always returns an "overwrite the cluster" command. As such, the order
    that iterables are listed in the initializer matter: their respective
    Actions are executed in order.
    Conceptually, this is very similar to:
        for x in X:
            for y in Y:
                for z in Z:
                    if (x, y, z) is a valid combination:
                        x()
                        y()
                        z()
    "is a valid combination" is defined as all Actions returning False to
    should_cancel, which is conditioned on the given walk. This let's an Action
    e.g. disqualify the walk if we have included a mirror protection Action,
    but the given Action does not make sense in the mirroring context.
    """
    def __init__(self, *iterables):
        self._iterables = iterables
        self._iterator = itertools.product(*iterables)
        self.walk_class = Walk
        self.rlock = threading.RLock()

    def __iter__(self):
        return self

    def next(self):
        with self.rlock:
            # Look for the next valid/not-canceled walk.
            # StopIteration will be raised naturally by self._iterator, so we
            # don't need to.
            walk_actions = None
            while walk_actions is None:
                new_walk_actions = self._iterator.next()
                for idx, action in enumerate(new_walk_actions):
                    if action.should_cancel(new_walk_actions, idx):
                        continue
                walk_actions = new_walk_actions

            return self.walk_class(walk_actions)

    def as_json(self):
        class json_iter(object):
            def __init__(inner_self):
                inner_self.sc = self

            def __iter__(inner_self):
                return inner_self

            def next(inner_self):
                encoded = encode.encode(inner_self.sc.next())
                return encoded

        return json_iter()

# PORT/WRAP: add back path, lin, file_config
class WalkOpTracer(central_logger.OpTracer):
    def trace(self, **op_info):
        sync_point = op_info.get('sync_point', None)
        walk = op_info['walk']
        walk_id = op_info['walk_id']

        info = {
                'walk_id': walk_id,
                'walk': walk,
                'sync_point': sync_point,
               }
        super(WalkOpTracer, self).trace(**info)

