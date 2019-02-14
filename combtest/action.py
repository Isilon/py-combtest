"""
This module provides ways of systematically generating operations and
combinations thereof. It is designed to allow for the systematic generation of
test cases.

Suppose we are doing whitebox testing of a GUI. The controls all have states,
such as whether a checkbox is checked, what the content of a text box is, etc.
The tuple of all the controls' states, and any other relevant state, is a
member of a thing we are call the 'state space'. The 'state space' contains
all possible states of the GUI; meaning: all possible combinations of the
states of the radio buttons, checkboxes, etc.

Conceptually there is also an 'action space'; it is the set of operations we
can use to interact with the GUI. The actions may in turn change the GUI's
state. For example, if our action is to click checkbox1, then its state
transitions from checked to unchecked or vice versa.

In general, the set of actions we can take may vary from state to state. For
example, if our GUI hides a checkbox because we have collapsed a menu, then any
action involving that checkbox is not available until we expand the menu.
Whether or not the menu is expanded is an aspect of the GUI's 'state'; thus we
have a different set of actions available to us depending on the GUI's state.

A generating test cases combinatorically (aka combtesting) is where we
systematically generate tests (known throughout this lib as 'Walks')
based on combinations of available Actions. In the graph theory world,
we think of this as systematically generating walks on a graph.

Laying out some pseudocode to illustrate it, suppose we have 3 lists of
function pointers: X, Y, Z. Each element of a given list is an operation we can
perform. So e.g. in the GUI case above, all elements of X may be different
operations we can do on a text box. If we want to generate a bunch of unique
series of operations, it may look like:

        for x in X:
            for y in Y:
                for z in Z:
                    if (x, y, z) is a valid combination:
                        x()
                        y()
                        z()

This should be abstract enough to be applicable to many
different domains.

An Action represents an operation performed by the test. A Walk is simply a
sequence of Actions to perform, and is analogous to a test case. The
central notion of combtesting is to generate a bunch of Walks based on
combinations of the supported operations. That results in combinatoric coverage
of the thing being tested.

Below I will refer to 'state/action spaces', which may be an unfamiliar term
to you. That is an AI-nerd's way of referring to the states that the feature
being tested can find itself in (collectively referred to as the 'state
space'), and the set of operations that cause a transition from the current
state to a new one (the operations are drawn from an 'action space'). The term
'Walk' comes from graph theory: we are 'walking' the 'graph' from state to
state by performing actions. This should be familiar to somebody trained in
basic AI models, e.g. Markov Decision Processes.

The ideas of action, state, walk, etc. are drawn from this sort of thinking. An
Action instance is a member of one of the lists above, and the list is provided
by MyActionClass.get_option_set().
"""
import copy
import random


class Action(object):
    """
    An Action class bundles a function that performs an operation, and all
    the different parameterizations for that operation. The run() method should
    be overriden; it performs the operation. The get_option_set() classmethod
    should be overriden to provide all the valid parameterizations of the
    operation.

    If you won't know until runtime if an operation is valid, you
    can test for that in run

    So for
    example, if you are testing a file system, perhaps the options for a
    'write' Action would

    actual transition in state/action space, or a way
    of refining options for a later transition (e.g. select byte range of file
    to write). There are two ways to provide arguments +
    modulate behavior based on state: 1. a static context provided to the
    initializer. This is an opportunity to provide arguments not conditioned
    on state, such as e.g. m, n, b, type for a protection level if the Action
    represents setting the protection level. Abstractly, the static ctx is
    immutable and forms part of the identity of the Action. 2. A dynamic ctx
    which can morph as state changes. This could be e.g. a container with a
    TestFile handle that we are actually executing the operations on.
    Actions are free to manipulate this dynamic ctx according to their own
    logic. Abstractly it is considered mutable, though the actual client
    code can use this ctx however it wants.
    """
    def __init__(self, ctx):
        self._static_ctx = ctx

    @property
    def static_ctx(self):
        # XXX it would be nice to have a copy protocol here, but we would have
        # to impose on the client code to have a copy-able ctx. So: we are
        # protecting our reference here but not the internal state of the thing
        # refered to. Up to the client code to make sure this is immutable.
        return self._static_ctx

    def __call__(self, ctx=None):
        self.run(self.static_ctx, dynamic_ctx=ctx)

    def should_cancel(self, walk, idx):
        """
        Args:
            walk - the walk we are in
            idx - the index we are at in the walk
        """
        return False

    def run(self, static_ctx, dynamic_ctx=None):
        raise NotImplementedError("Must implement this Action's run method")

    @classmethod
    def get_option_set(cls):
        raise NotImplementedError("Must provide a set of options this Action "
                "type supports")

    def to_json(self):
        return self.static_ctx

    @classmethod
    def from_json(cls, obj):
        return cls(ctx=obj)

class SyncPoint(Action):
    """
    SyncPoint: a type of Action which cannot be run in parallel with others,
               e.g. for when we are running a bunch of 'Walks' in parallel.
               Thinking in terms of parallel algorithms: this represents a
               pinch point where an operation must be serialized with
               everything else. In terms of testing: it is part of a test
               where we can't run other cases in parallel; e.g. if we are
               twiddling some global config option that affects all
               running test cases.
    """
    def __init__(self, *args, **kwargs):
        self.is_nop = False
        super(SyncPoint, self).__init__(*args, **kwargs)

    def __call__(self, ctx=None, **_kwargs):

        if ctx is not None:
            if not isinstance(ctx, dict):
                # We can lock this type with Python3 annotations
                raise ValueError("ctxs need to be dicts")
            ctx_copy = copy.copy(ctx)
        else:
            ctx_copy = {}

        if _kwargs:
            if ('branch_id' not in _kwargs or
                'service' not in _kwargs or
                'worker_ids' not in _kwargs or
                'epoch' not in _kwargs):
                    raise ValueError("I don't recognize these kwargs; did you "
                            "mean to pass these? %s" % str(_kwargs))
            ctx_copy['update_remote_ctx'] = _kwargs

        self.run(self.static_ctx, dynamic_ctx=ctx_copy)

        try:
            del ctx_copy['update_remote_ctx']
        except KeyError:
            pass

        ctx_copy = ctx_copy or None
        return ctx_copy

    def update_remote_contexts(self, dynamic_ctx, **kwargs):

        if ('update_remote_ctx' not in dynamic_ctx or
                dynamic_ctx['update_remote_ctx']['epoch'].level == 0):
            # Either we are running locally (e.g. for replay) and so we have no
            # remote contexts, or this is syncpoint is being run before any
            # other actions, so we don't yet have any remote ctxs established.
            # Either way, we straight-up update the local ctx. If we later
            # begin remote execution, the value will get passed to the
            # appropriate walks via the ctx (via scatter_work).
            dynamic_ctx.update(kwargs)
        else:
            service = dynamic_ctx['update_remote_ctx']['service']
            epoch = dynamic_ctx['update_remote_ctx']['epoch']
            # Maps walk_id->ip
            id_map = service.id_map

            # Maps ip->walk_id list
            walks_to_update = {}
            for walk_id in range(epoch.walk_idx_start, epoch.walk_idx_end):
                ip = id_map[walk_id]
                if ip not in walks_to_update:
                    walks_to_update[ip] = []

                walks_to_update[ip].append(walk_id)

            for ip, walk_ids in walks_to_update.iteritems():
                try:
                    service.update_remote_contexts(ip,
                            dynamic_ctx['update_remote_ctx']['worker_ids'],
                            walk_ids, **kwargs)
                except Exception as e:
                    new_msg = str(e) + "\nIP was: %s" % ip
                    raise RuntimeError(new_msg)

    def run_as_replay(self, static_ctx, dynamic_ctx):
        raise NotImplementedError()

    def replay(self, dynamic_ctx):
        return self.run_as_replay(self.static_ctx, dynamic_ctx=dynamic_ctx)


class OptionSet(object):
    """
    An OptionSet can be thought of as a "term" in the cartesian product formed
    by a StateCombinator. Example: an OptionSet may provide all protection
    levels we are interested in testing. An OptionSet can be provided as one of
    the 'iterables' of a StateCombinator.
    """
    def __init__(self, options, action_class):
        """
        Args:
            options - an iterable of 'options', whatever that means to the
                      given action class. Options will be provided as the
                      Action's static context.
        """
        # Let's control the class choice a bit here to prevent encapsulation
        # leak, and monkeying later. This could give us an easier path to
        # updating Action and using that update here. If we let the user
        # provide any class whose instances are callable we could lose that
        # degree of freedom.
        assert issubclass(action_class, Action)

        self._options = options
        self._iter = iter(options)
        self._action_class = action_class

    def __iter__(self):
        return self

    def next(self):
        """
        Lazy iteration, meaning: generate the actual Actions on the fly to try
        to minimize mem footprint.
        Raises:
            StopIteration when the underlying options iterator is exhausted
        """
        next_option = self._iter.next()
        return self._action_class(next_option)

    def to_json(self):
        return (self._options, utils.get_class_qualname(self._action_class))

    @classmethod
    def from_json(cls, obj):
        try:
            options, action_class_qualname = obj
            action_class = get_class_from_qualname(action_class_qualname)
        except IndexError:
            raise ValueError("Cannot interpret this as an OptionSet: %s" %
                    str(obj))

        return cls(options, action_class)

def get_random_action_by_class(action_class):
    option_set = action_class.get_option_set()
    choice = random.choice(list(option_set))
    return choice
