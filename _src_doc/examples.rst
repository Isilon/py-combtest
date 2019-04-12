Example code
====================

The first step we need is to define the operations that we will be combining.
In combtest these are called ``Actions``. Each is a decendent class
of :class:`Action`. Such classes require a ``run()`` function that defines
the actual operation. Acceptable parameters for passing to the function can
be provided via the ``OPTIONS`` class member.

A test case is represented as a sequence of :class:`Action` instances. These
cases are :class:`Walk` instances, which will be generated via combination of
the ``Actions`` the user provides. ``Walks`` are simply executables that
execute the ``run()`` functions of their ``Actions`` in order, passing a
shared object called ``state`` to each.

Example: ::

    from combtest.action import Action

    class MyAction(Action):

        # Each value in this list can be passed to run(). When we
        # generate test cases we will use this option list, and the option
        # list of all other Actions, and generate all possible combinations.
        OPTIONS = [True, False]

        def run(self, param, state=None):
            # The 'state' is an optional object passed among all instances in a
            # ``Walk``. It can be anything the user wants to have shared across
            # those ``Actions``. A copy is made for each generated ``Walk``.
            something = state['something']
            something.method(param)

            # Raising an Exception during execution fails the case, and no
            # later ``Actions`` in the case will be run.
            if something.value == False:
                raise ScaryError(":(")


If you prefer to generate the options of an ``Action`` in some more interesting
way, or want to return an iterator of options, you can override the
:func:`get_option_set` classmethod: ::

    from combtest.action import Action, OptionSet

    import my_lib

    class MyAction(Action):
        def run(self, param, state):
            blah blah

        @classmethod
        def get_option_set(cls):
            # An OptionSet is an iterator that produces, in this case, ``cls``
            # instances, one for each value produced by the provided iterator.
            return OptionSet(my_lib.some_iterator, cls)


That is it; once you have defined your :class:`Action` classes, you can pass
them to the ``py-combtest`` framework to be run.


Generating and Running Tests
==============================

Once you have written your :class:`Action` classes, running tests is as simple
as: ::

    from combtest.runner import run_tests

    action_order = [
                    MyAction1,
                    MyAction2,
                    MyAction3,
                    MyAction4,
                   ]

    results = run_tests(action_order)
    print("Ran %d walks in %0.2fs with %d errors" % (results.walk_count,
                                                     results.elapsed,
                                                     results.error_count)

Tests will be generated automatically given the options that your ``Actions``
provide.

:func:`run_test` has many options, which are worth reading in the docs. Here
are a few examples.

Providing a custom ``state`` object: ::
    # You can provide some instance to serve as the state passed around during
    # the tests. There are two important details to know about this:
    #  * The state must be JSON-ifiable, but py-combtest provides a convenience
    #    pattern to help with that. See ``Notes on Serialization`` below.
    #  * Shallow copies of the state will be made, via copy.copy(), since each
    #    test owns its own copy. You may want to e.g. override __copy__ if
    #    the details of the copy are important to you.
    # Here is an example of a silly state class for simplicity of illustration:
    class MyStateClass(dict):
        @property
        def A(self):
            return self.get('A', None)

        @A.setter
        def A(self, value):
            self['A'] = value

        @property
        def B(self):
            return self.get('B', None)

        @B.setter
        def B(self, value):
            self['B'] = value


    class MyActionA(Action):
       OPTIONS = [1, 2, 3]

       def run(self, param, state):
           state.A = param

    class MyActionB(MyActionA):
       def run(self, param, state):
           state.B = param


    my_state = MyStateClass()
    # gather_states=True means that the resultingn states are interesting to
    # us, so we want the framework to pull the states back to us after the
    # test run.
    results = run_tests([MyActionA,
                         MyActionB],
                        state=my_state,
                        gather_states=True)

    print("Resulting states: ", results.states)

The output for this case looks like this: ::
    Resulting states: [[{u'A': 1, u'B': 1}, {u'A': 2, u'B': 1}, {u'A': 3, u'B': 1}], [{u'A': 1, u'B': 2}, {u'A': 2, u'B': 2}, {u'A': 3, u'B': 2}], [{u'A': 3, u'B': 3}, {u'A': 1, u'B': 3}, {u'A': 2, u'B': 3}]]

You can vary the log directory and verbosity of logs, and can record replays
of each ``Walk`` so you can re-run them later. ::

    # Minimalist logging. Verbose can be 0, 1, 2, with increasing log verbosity
    run_tests(...,
              verbose=0,
              ...
             )

    # At verbosity=2 we will produce an additional debug level log
    # If log_dir is provided we will also record a replay for every
    # ``Walk`` that is run. Paths to the log files will be available in
    # the Results object returned from ``run_tests()``.
    run_tests(...
              verbose=2,
              log_dir='/my/log/dir',
              ...
             )


Serial Actions
========================

Sometimes we need to perform an operation that affects multiple tests that we
want to run in parallel. py-combtest provides a feature for accomplishing that.

By way of example: suppose we are performing a system test, and that the system
has some global config setting that affects all tests. First We need all tests
to finish executing up to the point that the config change should happen. At
that point we perform the config change once and for all, and then release the
next part of the tests to once again run in parallel.

The operation that needs to run once-and-for-all and serial is represented
with a :class:`SerialAction`. The user provides their ``SerialAction`` in
exactly the same way they provide other ``Actions``. ::

    from combtest.action import SerialAction

    import my_system


    class ChangeConfigSerialAction(SerialAction):
        # A single option means we will be running this once for all
        # tests, but it is perfectly fine to have multiple options.
        OPTIONS = [True,]

        def run(self, param, state):
            # Change some global setting
            my_system.set_some_global_config(1)

    run_tests([Action1,
               Action2,
               ChangeConfigSerialAction,
               Action3],
              ...)



Notes on Serialization
========================

Several objects need to be serialized to work properly, such as custom
``state`` objects. ``py-combtest`` uses a callback pattern to make it
easy for the user to provide JSON serializers for any classes they
are using.  The serialization looks like this: ::

    class MyClass(object):
        def __init__(self, a, b):
            self.a = a
            self.b = b

        def to_json(self):
            return (self.a, self.b)

        @classmethod
        def from_json(cls, obj):
            cls(obj[0], obj[1])

The user provides the pair of methods ``to_json``, ``from_json``. The
former provides some JSON-ifiable object. ``py-combtest`` will recursively call
``to_json`` on anything contained in that object. So if ``self.a`` is another
custom class with a ``to_json`` method, the above example will still work.

The latter receives a copy of that object during deserialization and returns
a deserialized instance of the given class.

Logging and Replay
====================

Config
========

Notes on Scalability
========================

Custom Executor Bootstrapping Methods
=======================================
