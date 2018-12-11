"""
This module provides ways of systematically generating operations and
combinations thereof. This should be abstract enough to be applicable to many
different domains.

    Action: represents a specific operation + the specific arguments passed to
            that operation. Example: "write byte range of file" and "the byte
            range to write"
    OptionSet: a set of arguments that could be passed to an Action of a given
               type. Example: a list of protection policy strings.
"""

import random


class Action(object):
    """
    An Action represents an actual transition in state/action space, or a way
    of refining options for a later transition (by e.g. selecting some byte
    range). This could be e.g. setting the protection of the file, doing a
    write on it, or whatever. There are two ways to provide arguments +
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


class SyncPoint(Action):
    def __init__(self, *args, **kwargs):
        self.is_nop = False
        super(SyncPoint, self).__init__(*args, **kwargs)

    def update_remote_contexts(self, dynamic_ctx, **kwargs):
        service = dynamic_ctx['service']
        epoch = dynamic_ctx['epoch']
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
                service.update_remote_contexts(ip, dynamic_ctx['worker_ids'],
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


def get_random_action_by_class(action_class):
    option_set = action_class.get_option_set()
    choice = random.choice(list(option_set))
    return choice
