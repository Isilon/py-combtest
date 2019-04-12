"""
This module provides ways of systematically combining operations, sets of
operations, etc., and executing such sequences of operations.
"""
import copy
from collections import Iterable
import functools
import itertools
import operator
import threading
import traceback

from six import string_types

from combtest.action import SerialAction, Action, CancelWalk
import combtest.central_logger as central_logger
from combtest.central_logger import logger
import combtest.encode as encode
from combtest.utils import RangeTree


class WalkFailedError(RuntimeError):
    """
    Raised when a :class:`combtest.walk.Walk` failed to execute to completion
    for any reason (e.g. one of its operations raised an ``Exception``).
    """
    pass

class Walk(object):
    """
    A Walk, named after the graph theory concept, is a list of
    :class:`combtest.action.Action` to execute, together with a piece of
    state that the Actions can manipulate. The notion here is that each
    ``Action`` performs an operation that takes us through a transition
    in state/action space.

    :param iterable elems: An iterable of :class:`combtest.action.Action`
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
        """
        Logically equivalent to list.append
        """
        self._elems.append(elem)

    def __len__(self):
        return len(self._elems)

    def __eq__(self, other):
        if not isinstance(other, Walk):
            return False
        return other._elems == self._elems

    def __add__(self, other):
        """
        :return: concatenation of this ``Walk`` and another
        """
        if not isinstance(other, Walk):
            raise TypeError("Cannot add a Walk with object of type %s" %
                            str(type(other)))
        elems = self._elems + other._elems
        return self.__class__(*elems)

    def __iter__(self):
        return iter(self._elems)

    def execute(self, state, log_errors=True):
        """
        Execute the ``Actions`` in order. If an Action raises
        :class:`CancelWalk`, we will stop executing immediately.
        :return: True if the Walk was run successfully or `CancelWalk` was raised, False otherwise.
        """
        try:
            for op in self:
                # I will leave it up to the user to decide to log or not, and
                # the appropriate verbosity.
                op(state=state)
        except CancelWalk as e:
            # Likewise here: let the user decide to log or not in their layer
            return True
        except Exception as e:
            # PORT/WRAP: no state specifics here.
            msg_state = "Walk was: %s\nstate: %s" % (repr(self),
                    encode.encode(state))
            exc = type(e)(str(e) + "\n" + msg_state)
            if log_errors:
                logger.exception(exc)
                logger.error(msg_state)

            new_msg = "Walk failed:\n"
            new_msg += traceback.format_exc()
            new_msg += "\n" + msg_state
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
        assert not isinstance(obj, string_types), str(obj)
        out = cls()
        for act in obj:
            out.append(act)

        return out


class Segment(object):
    """
    This represents a collection of ``Walk`` portions to run. Example: if you
    have 300 Walks made of: [Action1, Action2, SerialAction1, Action3], the first
    Segment will be the [Action1, Action2] portion of every one of those 300
    walks. After running that Segment, we would then run the SerialAction.

    A Segment is: (sync_point_instance, [list, of, Action option sets, ...])

    It's iterator produces the Walk portions + consistent indexing of those
    portions so that a later portion can reclaim the earlier portion's state.

    :param int walk_count: the total number of Walks this Segment will produce
                           portions for.
    :param iterable options: An iterable of Action sets, as you'd get from
                             MyActionType.get_option_set()
    :param SerialAction sync_point_instance: SerialAction instance to run before the
                                          Walk portions.
    :param int parent_period: how many options our parent segment is tracking
    :param int level: Effectively: how many segments came before this segment
    """
    # Presume not threadsafe

    def __init__(self, walk_count, options=(),
                 sync_point_instance=None,
                 parent_period=1,
                 level=0):

        assert options or sync_point_instance
        assert sync_point_instance is None or isinstance(sync_point_instance,
                                                         SerialAction)

        self._walk_count = walk_count
        self._count_walks_produced = 0
        self._level = level

        # Run *after* the SerialAction, if there is one. That means that if this
        # Segment represents the leaf of the tree and we have a SerialAction,
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
        # SerialActions at all.
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

            check = self._count_walks_produced == self._walk_count

            try:
                self._held_walk = self._state_combinator.next()
            except StopIteration:
                self._refresh_state_combinator()
                self._held_walk = self._state_combinator.next()
            else:
                if check:
                    raise RuntimeError("Should have exhausted iterator")

        return walk

    def __next__(self):
        return self.next()


class Epoch(object):
    """
    An Epoch wraps a bunch of Walk portions that can run in parallel, and
    provides them consistent walk_id and branch_id.
    """
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

    def __next__(self):
        return self.next()


class WalkOptions(object):
    """
    A WalkOptions accepts a set of :class:`Action` and :class:`SerialAction`
    options and produces an iterable of :class:`Epoch`. Each `Epoch` represents
    a set of ``Walk`` segments which can run in parallel, and an optional
    ``SerialAction`` which should be run *before* the ``Walk`` portions.

    Not threadsafe.

    :param iterable walk_order: An iterable :class:`Action` types
    """
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
            if issubclass(action_class, SerialAction):
                self._sync_point_idxs.append(idx)

                # Append any action-ish segments, and the sync point
                actions = walk_order[seg_start_idx:idx]
                if actions:
                    actions = tuple([tuple(ac.get_option_set()) for ac in
                                     actions])
                    self._segment_options.append(actions)
                # else e.g. idx = 0: the first action_class is a SerialAction
                self._segment_options.append(action_class)

                seg_start_idx = idx + 1

            option_set = tuple(action_class.get_option_set())
            self._sizes.append(len(option_set))

        # Tail flush, if we haven't already.
        if not issubclass(action_class, SerialAction):
            actions = walk_order[seg_start_idx:]
            if actions:
                actions = tuple([tuple(ac.get_option_set()) for ac in
                                 actions])
                self._segment_options.append(actions)

        self.walk_count = functools.reduce(operator.mul, self._sizes)
        # During iteration, set to a list of:
        #  [(segment, start_idx, end_idx)]
        self._frontier = None

    @property
    def sizes(self):
        """
        :return: A tuple of the size of the option sets for each
                 :class:`Action` type in this ``WalkOptions``.
        """
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
            count = functools.reduce(operator.mul, [len(wo) for wo in current_options])

        return walk_options, idx, count

    def _ensure_tree_impl(self, walk_count, segment_options, start_idx=0,
                          period=1, level=0):
        # Called recursively with SerialAction at start, or this is the first
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
                                              segment_options,
                                              start_idx=end_idx,
                                              period=(period * walk_option_count),
                                              level=(level + 1))

            new_segment = Segment(walk_count, options=walk_options,
                                  parent_period=period, level=level)
            for child in children:
                new_segment.add_child(child)

            root_segments.append(new_segment)
        else:
            current_sync_point = segment_options[start_idx]
            sync_points = [instance for instance in
                           current_sync_point.get_option_set()]
            sync_point_count = len(sync_points)

            assert (walk_count % sync_point_count) == 0
            segment_walk_count = walk_count // sync_point_count

            walk_options, end_idx, walk_option_count = \
                    self._get_next_options(segment_options,
                                           start_idx=(start_idx + 1))
            assert walk_option_count == 0 or \
                    (segment_walk_count % walk_option_count) == 0

            children = self._ensure_tree_impl(segment_walk_count,
                                              segment_options,
                                              start_idx=end_idx,
                                              period=(period * walk_option_count),
                                              level=(level + 1))

            for sync_point in sync_points:
                new_segment = Segment(segment_walk_count, options=walk_options,
                                      sync_point_instance=sync_point,
                                      parent_period=period,
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
                current_value = segment.sync_point.param
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
            walks_per_child = expected_walk_count // len(children)

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
            walks_per_segment = self.walk_count // segment_count
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
            # SerialAction values are effectively no-ops, and in that case we
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

    def __next__(self):
        return self.next()


class StateCombinator(object):
    """
    A StateCombinator gives us all combinations of elements from the provided
    iterables. Simple cartesian product of the sets for the most part. The only
    exception is that it will toss out any Walks for which one of the
    :class:`Action` returns ``action_instance.should_cancel() == True``.
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
            # Look for the next valid/not-cancelled walk.
            # StopIteration will be raised naturally by self._iterator, so we
            # don't need to.
            walk_actions = None
            while walk_actions is None:
                new_walk_actions = next(self._iterator)
                for idx, action in enumerate(new_walk_actions):
                    if action.should_cancel(new_walk_actions, idx):
                        continue
                walk_actions = new_walk_actions

            return self.walk_class(walk_actions)

    def __next__(self):
        return self.next()

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

# TODO PORT/WRAP: add back path, lin, file_config
class WalkOpTracer(central_logger.OpTracer):
    """
    Traces a :class:`Walk` portion + its adjacent :class:`SerialAction` and
    ``walk_id``. The id is consistent across portions so that you can relate
    them back together later.
    """
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

