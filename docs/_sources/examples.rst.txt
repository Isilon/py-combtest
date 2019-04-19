Example code
====================

The first step we need is to define the operations that we will be combining.
In combtest these are called ``Actions``. Each is a decendent class
of :class:`~combtest.action.Action`. Such classes require a
:class:`~combtest.action.BaseAction.run` function that defines the actual
operation. Acceptable parameters for passing to the function can
be provided via the ``OPTIONS`` class member.

A test case is represented as a sequence of :class:`~combtest.action.Action`
instances. These cases are :class:`~combtest.walk.Walk` instances, which
will be generated via combination of the ``Actions`` the user provides.
``Walks`` are simply executables that execute the :func:`~combtest.action.BaseAction.run`
functions of their ``Actions`` in order, passing a shared object called
``state`` to each.

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
:func:`~combtest.action.BaseAction.get_option_set` classmethod: ::

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


That is it; once you have defined your :class:`~combtest.action.Action`
classes, you can pass them to the ``py-combtest`` framework to be run.


Generating and Running Tests
------------------------------

Once you have written your :class:`~combtest.action.Action` classes, running
tests is as simple as: ::

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

:func:`~combtest.runner.run_tests` has many options, which are worth reading
in the docs. Here are a few examples.

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
of each :class:`~combtest.walk.Walk` so you can re-run them later. ::

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
------------------------

Sometimes we need to perform an operation that affects multiple tests that we
want to run in parallel. ``py-combtest`` provides a feature for accomplishing
that.

By way of example: suppose we are performing a system test, and that the system
has some global config setting that affects all tests. First We need all tests
to finish executing up to the point that the config change should happen. At
that point we perform the config change once and for all, and then release the
next part of the tests to once again run in parallel.

The operation that needs to run once-and-for-all and serial is represented
with a :class:`~combtest.action.SerialAction`. The user provides their
``SerialAction`` in exactly the same way they provide other
``Actions``. ::

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

Notice above that the ``SerialAction`` is treated like any other. The framework
will run all ``Action1`` and ``Action2`` in parallel, wait for them to finish,
run ``ChangeConfigSerialAction`` once, then run all ``Action3`` in parallel. If
the ``SerialAction`` had more than one option, each option will be run one at a
time, serially.

Notes on Distributed Test Execution
-------------------------------------

``py-combtest`` provides a number of ways to execute tests in a distributed +
parallel fashion. The mechanisms for that are as follows:

 * A generalized thread pool implementation that can execute multiple callables
   in parallel in a given process. (:class:`~combtest.worker.ThreadPool`)
 * An ``rpyc`` -based service which receives lists of tests (or portions
   of tests) to run, dispatches them to a thread pool for for execution, and
   which collects outputs, statistics, etc.
   (:class:`~combtest.worker.CoordinatorService`)
 * A client class that bootstraps test executors, potentially across many
   machines, and dispatches tests to them for running.
   (:class:`~combtest.worker.ServiceGroup`)
 * Customizable mechanisms for bootstrapping the services - e.g. via SSH,
   multiprocessing. (:class:`~combtest.bootstrap.ServiceHandler`)

For most purposes the user should not need to inherit or override any of these,
but they are designed to make that possible. For convenience, all overrides can
be made at the :func:`~combtest.runner.run_tests` call. Example: ::

    class MyTestService(MultistageWalkRunningService):
        ...

    ...

    run_tests(..., runner_class=MyTestService)

Using SSH To Bootstrap Test Executors Across Machines
+++++++++++++++++++++++++++++++++++++++++++++++++++++++

The user is free to specify how test executors are bootstrapped, but two
convenience implementations are provided:

 * A ``multiprocessing``-based method, which spins up multiple processes on the
   local machine. This is the default implementation.
 * An optional ``paramiko``-based SSH implementation, which makes it relatively
   simple to bootstrap test executors across machines.

The latter is used by simply installing it and passing it to
:func:`~combtest.runner.run_tests`: ::

    import combtest.ssh_handle as ssh

    ...

    run_test(..., service_handler_class=ssh.ServiceHandler_SSH)

The trick to using this is the need to provide authentication. Authentication
credentials can be specified using the :class:`~combtest.config` module
(see below). A path to an rsakey file can be provided for example.
``paramiko`` also supports username/password authentication, among other
methods.

NOTE: ``paramiko`` is not installed by default with ``py-combtest``. You will
need to ensure it is installed to use :class:`~combtest.ssh_handle`.

Custom Service Bootstrapping Methods
+++++++++++++++++++++++++++++++++++++++

The user is free to specify other ways of bootstrapping test executors,
though that will not be required for most users. See the interface provided
by :class:`~combtest.bootstrap.ServiceHandler` for reference.

Notes on Serialization
------------------------

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
a deserialized instance of the given class. That is: it maps the thing
returned by ``to_json`` on the class's constructor.

Logging and Replay
--------------------

There are three main types of logging ``combtest`` uses:
 * Standard Python logging, which the user can leverge as normal
 * An extension to Python logging, called :class:`~combtest.central_logger`;
   the user can treat this like standard Python logging, but logs will
   automatically be sent between the test execution services and the
   coordinating process (where :func:`~combtest.runner.run_tests` was called).
 * Human-readable representations of test cases; these service as traces of
   the cases which were run, and can be fed back to the
   :class:`~combtest.replay` module to be re-run individually.

Distributed Logging
+++++++++++++++++++++
To log using :class:`~combtest.central_logger`, simply leverage the logger
it exposes instead of using a logger from the standard logging lib: ::

    from combtest.central_logger import logger

    ...

    logger.error("We are having trouble here.")

These log lines will automatically be dispatched back to the coordinating
process, where they may be printed to e.g. stdout and/or to a log file,
depending on the verbosity settings the user chose.

Replay
++++++++

We collect human-readable traces of the tests we run whenever the user
provides a log directory to :func:`~combtest.runner.run_tests`. After the
tests run, we can pass the logs, log string, or :class:`~combtest.walk.Walk`
to the :class:`~combtest.replay` module to run an individual case.

Running a single :class:`~combtest.walk.Walk`: ::

    import combtest.replay as replay
    import combtest.walk as walk

    import my_test

    actions = [
               my_test.ActionClass1(1),
               my_test.ActionClass2(True),
               my_test.SerialActionClass1("qqq"),
              ]
    my_walk = walk.Walk(actions)

    my_state = {"some": "stuff}
    replay.replay_walk(my_walk, state=my_state)

Re-running a :class:`~combtest.walk.Walk` from a trace file: ::

    # Suppose we run some tests, and some of them failed:
    results = run_test(...)

    # We can replay one of the failed tests from the trace logs:
    my_state = {"some": "thing"}
    replay.replay_walk_by_id(results.logs, results.failed_tests[17],
                             state=my_state)

You can actually run any :class:`~combtest.walk.Walk` from the trace, not
just failed ``Walks``. The second argument to
:func:`~combtest.replay.replay_walk_by_id` is an integer identifier of
the ``Walk``, which can be found in the trace file.

Config
--------

``py-combtest`` uses a minimalist config for parameterizing things like
thread counts and authentication information for machines where test
executors should run. The values can be set at runtime or by config
file.

By default, :class:`~combtest.config` will look for a file called
``combtest.cfg`` in the current working directory and attempt to load it. It
could be used for example to specify the maximum number of worker threads the
test executors are allowed.  When using SSH to bootstrap the test executors,
``config`` can be used to provide authentication information. Example file: ::

    [NET]
    rsakey: /path/to/my/rsakey/file

    [WORKER]
    max_thread_count: 10

The values can also be overridden programatically by the user: ::

    import combtest.config as config

    # Default port we will bind our rpyc instance to on a given machine. If we
    # bind more than one on a given machine, this will be the first port, and
    # all ports will be consecutive.
    config.set_service_port(6009)
