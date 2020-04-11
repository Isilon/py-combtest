py-combtest
=======================================

The term 'combtesting' refers to the systematic generation of test cases based
on combinations of inputs, conditions, configs, etc. This lib supports the
generation of cases in this way, and provides a metric ton of supporting tools
that a writer of such tests is likely to need. ::

  import sys

  import combtest.action as action
  import combtest.runner as runner

  from mylib import MyClass
  from mylib.test.utils import check_sanity


  # Suppose we want to test a bunch of parameterizations of MyClass, whose
  # initializer accepts a single str parameter, and which has a method called
  # run() which accepts a single int parameter. And let's say we have written
  # a utility function called check_sanity() that we use during a test to
  # make sure our instance is sane given its current parameters.


  class ActionInitialize(action.Action):
      # Some strings to pass to the initializer. These will be passed to the
      # __init__ as the param. If you want a more complex method to generate
      # options, simply override get_option_set().
      OPTIONS = ['', ' ', '23452345', 'sdfasdfas', u'\xaa'.encode()]

      def run(self, param, state):
          # Any exceptions raised here will be caught by the framework and
          # counted as errors.
          my_instance = MyClass(param)

          # Let's presume our check_sanity function will raise
          # exceptions on failure
          check_sanity(my_instance)

          # Save the instance for the next Action, allowing us to test
          # all combinations of weird strings and weird run() params.
          state['instance'] = my_instance

  class ActionRun(action.Action):
      # Some interesting ints to test
      OPTIONS = [0, 1, -1, sys.maxsize, -sys.maxsize - 1]

      def run(self, param, state):
          my_instance = state['instance']
          my_instance.run(param)
          check_sanity(my_instance)

  my_actions = [
                ActionInitialize,
                ActionRun,
               ]

  # run_tests() actually generates the test cases and runs them. By default
  # it will spin up several processes locally so we can run the tests in
  # parallel. Optionally, it can spin up rpyc-based services across N nodes
  # for distributed execution.
  result = runner.run_tests(my_actions, state={})
  if result.error_count > 0:
      sys.stderr.write("We have %d failures!\nSee logs: %s\n" %
                       (result.error_count, result.logs))


.. toctree::
   :maxdepth: 2
   :caption: Quick start

   introduction
   examples

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
