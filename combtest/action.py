"""
This module provides the core classes for specifying operations and
systematically generating combinations of them.
"""
import copy
import random

import combtest.utils as utils



class CancelWalk(RuntimeError):
    """
    Raised to immediately stop the execution of a :class:`combtest.walk.Walk`.
    """
    pass


class BaseAction(object):
    """
    An Action class bundles a function that performs an operation, and all
    the different parameterizations for that operation. The
    :func:`~combtest.action.Action.run` method should be overriden; it performs
    the operation. The :func:`~combtest.action.Action.get_option_set()`
    classmethod should be overriden to provide all the valid parameterizations
    of the operation.

    If you won't know until runtime if an operation is valid, you can test for
    that in ``run()``, and raise a :class:`combtest.walk.CancelWalk` exception if
    you want to stop executing that Walk right away. You may want to cancel,
    for example, if the operation + parameter does not make sense given the
    current state of the feature. Giving that a solid example: suppose you are
    testing a GUI and it has a control hidden, and suppose the operation is to
    interact with that control. In the ``run()`` method you can test for that
    condition and raise a :class:`combtest.walk.CancelWalk`.

    State is represented as an object provided by the user, which will be
    passed to :func:`~combtest.action.Action.run` call.
    """
    def __init__(self, param):
        """
        The initializer takes a single argument: an object defining the
        parameters to pass to the operation (which is defined in the
        py:meth:`combtest.action.Action.run` function. This should not be changed once the
        object is initialized. Thus: an Action instances comes to be an
        immutable object representing a specific operation with its
        spcific parameters.

        :param object state: an object that will be passed to the
                             :func:`~combtest.action.Action.run` method later.
        """
        self._param = param

    @property
    def param(self):
        """
        This instance's parameters, as passed to the initializer.
        """
        # XXX it would be nice to have a copy protocol here, but we would have
        # to impose on the client code to have a copy-able state. So: we are
        # protecting our reference here but not the internal state of the thing
        # refered to. Up to the client code to make sure this is immutable.
        return self._param

    def __call__(self, state=None):
        self.run(self.param, state=state)

    def should_cancel(self, walk, idx):
        """
        This method is called before a Walk containing this Action is run. This
        gives an opportunity to prevent the Walk from running if this
        combination just doesn't make any sense.

        :param Walk walk: the walk we are in
        :param int idx: the index we are at in the walk
        :return: True if the Walk should not be run, else False
        """
        return False

    def run(self, param, state=None):
        """
        This is called to actually run the operation. It does not need to be
        invoked directly by the user.

        :param object param: this represents the paramaterization of the
                                  operation. For example: if you are testing
                                  a GUI and this Action represents "set
                                  the state of checkbox1", then param
                                  would be the checkbox1 setting.

        :param object state: this object is passed along from
                                   Action-to-Action in a given Walk as the
                                   Actions execute. It represents any state
                                   that needs to be provided to the Action,
                                   or which may be modified by the Action.
                                   If you are testing a GUI for example,
                                   perhaps this is a handle to an instance of
                                   the GUI.

        :raises :class:`combtest.walk.CancelWalk`: the operation can raise
        :class:`combtest.walk.CancelWalk` to cancel the execution of a
        :class:`combtest.walk.Walk` at this point.
        """
        raise NotImplementedError("Must implement this Action's run method")

    @classmethod
    def get_option_set(cls):
        """
        Return the full parameterization space for this Action type. For
        example: if you are testing a GUI and this Action sets the state of a
        checkbox, then perhaps this will return ``cls(True)`` and
        ``cls(False)`` for 'set the box to checked' and 'set the box to
        unchecked' respectively.

        Note:
            This returns cls instances, not the parameters themselves.

        :return: An iterable of instances of type cls
        """
        raise NotImplementedError("Must provide a set of options this Action "
                "type supports")

    def to_json(self):
        """
        Return something JSONifiable that can uniquely represent this instance.
        """
        return self.param

    @classmethod
    def from_json(cls, obj):
        return cls(param=obj)

class Action(BaseAction):
    """
    Convenience form of BaseAction: let the user provide a simple iterable of
    parameters, called ``OPTIONS``.
    """
    OPTIONS = None

    @classmethod
    def get_option_set(cls):
        """
        Return the full parameterization space for this Action type. For
        example: if you are testing a GUI and this Action sets the state of a
        checkbox, then perhaps this will return ``cls(True)`` and
        ``cls(False)`` for 'set the box to checked' and 'set the box to
        unchecked' respectively.

        Note:
            This returns cls instances, not the parameters themselves.

        :return: An iterable of instances of type cls
        """
        if cls.OPTIONS is None:
            raise NotImplementedError("Must provide a set of options this Action "
                    "type supports, either via cls.OPTIONS, or by overriding "
                    "this method.")

        return OptionSet(cls.OPTIONS, action_class=cls)

class SerialAction(Action):
    """
    A SerialAction is a type of Action which cannot be run in parallel with
    others, e.g. for when we are running a bunch of 'Walks' in parallel.
    Thinking in terms of parallel algorithms: this represents a pinch point
    where an operation must be serialized with everything else. In terms of
    testing: it is part of a test where we can't run other cases in parallel;
    e.g. if we are twiddling some global config option that affects all running
    test cases. We change the option once for all test cases after all
    outstanding work has finished.

    Fundamentally, a SerialAction is run a single time for a given
    parameterization, regardless of how many Walks contain it. The part of the
    Walks prior to the SerialAction will be run first.

    Example: suppose we have three Walks:
        * [Action1(1), Action2(2), SerialAction1(1), Action3(3)],
        * [Action1(1), Action2(3), SerialAction1(1), Action3(3)],
        * [Action1(1), Action2(2), Action3(3), SerialAction1(1), Action4(4)]

        We can run the first 'segment' of each Walk in parallel:
        * [Action1(1), Action2(2)],
        * [Action1(1), Action2(3)],
        * [Action1(1), Action2(2), Action3(3)]

        Then, single a thread we will run SerialAction1(1), then in parallal we
        can run:
        * [Action3(3)],
        * [Action3(3)],
        * [Action4(4)]
    """
    def __call__(self, state, branch_id=None, epoch=None, worker_ids=None,
                 service=None):
        if epoch is None or epoch.level == 0:
            state_copy = state
        else:
            state_copy = copy.copy(state)

        #if _kwargs:
        #    if ('branch_id' not in _kwargs or
        #        'service' not in _kwargs or
        #        'worker_ids' not in _kwargs or
        #        'epoch' not in _kwargs):
        #            raise ValueError("I don't recognize these kwargs; did you "
        #                    "mean to pass these? %s" % str(_kwargs))
        #    state_copy['update_remote_state'] = _kwargs

        try:
            new_state = self.run(self.param, state=state_copy)
        except CancelWalk:
            raise RuntimeError("Raising CancelWalk in a SerialAction doesn't "
                               "change behavior; just exclude its param "
                               "instead, or use should_cancel.")

        if new_state is not None:
            self._update_remote_states(state_copy, epoch=epoch,
                                       worker_ids=worker_ids,
                                       service=service)
            return new_state
        return state

    def _update_remote_states(self, state, epoch=None, worker_ids=None,
                              service=None):
        """
        Fundamentally, SerialActions are run a single time for each
        parameterization, as explained above. SerialAction instances can
        be executed in a totally different memory space than the rest of a
        :class:`combtest.walk.Walk` (e.g. due to using remote executors; see
        :class:`combtest.worker.ServiceGroup`). So to update the state
        passed along with the Walk, the run() method can call this function to
        push an update to the Walk's remote state.

        :param object state: this is the local state that was passed to
                                   the :func:`run` function.
        :param kwargs: these are key/value pairs to be pushed to the remote
                       state
        """
        if epoch is None or epoch.level == 0:
            # Either we are running locally (e.g. for replay) and so we have no
            # remote states, or this SerialAction is being run before any
            # other actions, so we don't yet have any remote states established.
            # Either way, we straight-up update the local state. If we later
            # begin remote execution, the value will get passed to the
            # appropriate Walks via the ``state`` arg (via scatter_work).
            return
        else:
            # Maps walk_id->connection_info
            id_map = service.id_map

            # Maps connection_info->walk_id list
            walks_to_update = {}
            for walk_id in range(epoch.walk_idx_start, epoch.walk_idx_end):
                connection_info = id_map[walk_id]
                if connection_info not in walks_to_update:
                    walks_to_update[connection_info] = []

                walks_to_update[connection_info].append(walk_id)

            for connection_info, walk_ids in walks_to_update.items():
                try:
                    service.update_remote_states(connection_info,
                                                 worker_ids,
                                                 walk_ids,
                                                 state)
                except Exception as e:
                    new_msg = str(e) + "\nIP was: %s" % str(connection_info)
                    raise RuntimeError(new_msg)

class OptionSet(object):
    """
    An :class:`OptionSet` is an iterable that provides instances of a given
    :class:`combtest.action.Action` class. This is typically used as a return
    value from an Action's :func:`get_option_set` method.
    """
    def __init__(self, options, action_class):
        """
        :param iterable options: an iterable of paramaterizations of the
                                 given Action class. There parameters will
                                 be provided as the Action's ``param``.

        :param Action action_class: The :class:`Action` this iterator will be
                                    returning instances of.
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

    def __next__(self):
        return self.next()

    def next(self):
        """
        Lazy iteration, meaning: generate the actual Actions on the fly to
        try to minimize mem footprint.
        :raise StopIteration: when the underlying options iterator is exhausted
        """
        next_option = next(self._iter)
        return self._action_class(next_option)

    def to_json(self):
        return (self._options, utils.get_class_qualname(self._action_class))

    @classmethod
    def from_json(cls, obj):
        try:
            options, action_class_qualname = obj
            action_class = utils.get_class_from_qualname(action_class_qualname)
        except IndexError:
            raise ValueError("Cannot interpret this as an OptionSet: %s" %
                    str(obj))

        return cls(options, action_class)

def get_random_action_by_class(action_class):
    """
    Get an instance of the given :class:`Action`, randomly chosen from the set
    its :func:`Action.get_option_set` produces.
    :return: An instance of ``action_class``.
    """
    option_set = action_class.get_option_set()
    choice = random.choice(list(option_set))
    return choice
