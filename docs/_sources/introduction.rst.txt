Overview
==================================================================

The term 'combtest' refers to the systematic generation of test cases based on
combinations of inputs, conditions, configs, etc. This lib supports the
generation of cases in this way, and provides a metric ton of supporting tools
that a writer of such tests is likely to need. Examples:

* Some basic test generation logic, so that the test writer can concentrate on
  writing the tests and not the infrastructure.
* Test runners that support various parallel execution patterns, which is
  highly desireable given the sheer volume of cases likely to be generated.
* ``unittest``-like test results stats
* Test case logging and deterministic replay mechanisms, allowing the user
  to re-run any generated test case.

These make it easy for a test writer to get started. At the same time,
they are written to allow the advanced user to easily build out a custom
workflow.

At each step, the lib was designed with a small handful of concerns in mind:

* Providing 'telescoping' APIs, such that both beginning and advanced users can
  leverage it in the way they need
* Scalability, particularly with respect to mem use, and providing remote
  test executors; combtesting can produce orders of magnitude more cases than
  traditional testing, which means scalability becomes a natural concern
* Paralellism of test execution, since running orders of magnitude more
  tests in serial would otherwise take orders of magnitude more time


Why this kind of testing?
==================================================================

Seasoned test writers may have come across a scenario like this: they develop a
set of N edge cases to test, and they hardcode ``unittest`` cases for those N;
then they come to realize that they really need to test some of the
combinations of those cases, and maybe then to combine all of those with
various configurations of their system. "But that is combinatorial explosion;
you can't test all that!" Nevertheless, in complex software systems it is
sometimes necessary to do such a combining of tests/configs/etc to have
confidence in the quality of the product. Even for simpler software systems,
this can net a number of important advantages:

* Like fuzz testing, you can get exponentially more coverage than you can
  with tests specifically enumerated by a tester.
* Unlike fuzz testing, you get guaranteed, verifiable coverage, just like
  specifically enumerated tests.
* You get built-in deterministic replay of the test, unlike with some styles
  of fuzz testing where that can be challenging.


High level example
==================================================================

Suppose we are doing whitebox testing of a GUI. The controls all have
states, such as whether a checkbox is checked, what the content of a text box
is, etc.

And let's say we want to excercise every distinct and supported
combination of control states. Distinctness here refers to states of the GUI
which are distinct from the point of view of the tester. Let's say that
entering the value "Hello world" in a text box fundamentally tests the same
thing as entering "Foo bar", but that having the box empty, or having numerical
data in the box, is different enough to constitute distinct test cases. Let's
say we come up with 50 such distinct values to try in that text box during
testing. This is the space of options we will try for this one control, and we
come up with the space of options we will try for all the others, such as a
check box being checked or not.

Note that some combinations may not be possible: if clicking a check box
reveals certain controls, those controls can only be interacted with if
the check box has been clicked. We will need to take this into account
while generating test cases.

Suppose we define a simple function that takes a GUI instance as an argument,
and text to enter in a specific text box on that GUI.  It might look like: ::

 def mess_with_textbox1(gui_instance, value):
     gui_instance.textbox1.value = value

Then we can iterate all the interesting values we came up with: ::

 for value in textbox1_values:
     gui_instance = get_an_instance_of_my_GUI()
     mess_with_textbox1(gui_instance, value)

This is simple enough that we could generate cases programmatically for use
in our favorite test framework like ``unittest``, ``pytest``, etc.

Now let's say that we have a bunch of other controls on the GUI, each with a
set of distinct states. Some combinations of these control states may not be
valid (like setting values in a text box that is not visible). And let's say
that we define functions for messing with these controls, like we did above.
The resulting pseudocode now looks like: ::

    for c1_state in c1_options:
        for c2_state in c2_options:
            for c3_state in c3_options:
                if (c1_state, c2_state, c3_state) is a valid combination:
                    g = get_an_instance_of_my_GUI()
                    mess_with_c1(g, c1_state)
                    mess_with_c2(g, c2_state)
                    mess_with_c3(g, c3_state)

There are a few other things we will probably want to go along with this:

* If it is much easier for us to figure out at runtime if a combination of
  operations is valid, then we may want to e.g. have a special exception or
  return value from our individual op functions that indicates we should
  immediately cancel execution of the test case.
* A framework to run all the different combinations and count passes and
  failures, etc.
* Mechanisms for running the cases in parallel if the user chooses, since
  we are likely to have a very large number of cases to run.
* Logging and replay mechanisms.

This lib provides the stuff you need to accomplish all of the above.


Comparison to unittest and pytest case generation
===================================================
TODO
