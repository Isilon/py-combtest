An Introduction To Combinatoric Test Case Generation (Combtesting)
==================================================================

Lorem ipsum whatever whatever TODO

The term 'combtest' refers to the systematic generation of test cases based on
combinations of inputs, conditions, configs, etc. The term is somewhat

also includes the
systematic generation of sequences
se inputs can be
sequenced in unique ways, or conditioned on the current state

Example:
Suppose we are doing whitebox testing of a GUI. The controls all have
states, such as whether a checkbox is checked, what the content of a text box
is, etc.  The tuple of all the controls' states, and any other relevant state,
is a member of a thing we can call the 'state space'. The 'state space'
contains all possible states of the GUI; meaning: all possible combinations of
the states of the radio buttons, checkboxes, etc.

Conceptually there is also an 'action space'; it is the set of operations we
can use to interact with the GUI. The actions may in turn change the GUI's
state. For example, if our action is to click checkbox1, then its state
transitions from checked to unchecked or vice versa.

In general, the set of actions we can take may vary from state to state. For
example, if our GUI hides a checkbox because we have collapsed a menu, then any
action involving that checkbox is not available until we expand the menu.
Whether or not the menu is expanded is an aspect of the GUI's 'state'; thus we
have a different set of actions available to us depending on the GUI's state.

Generating test cases combinatorically (aka combtesting) is where we
systematically generate tests (known throughout this lib as 'Walks') based on
combinations of available actions x states.  In the graph theory world, we
think of this as systematically generating walks on a graph, hence the use of
graph theory terminology we use in some places in this lib.

Laying out some pseudocode to illustrate it, suppose we have 3 lists of
function pointers: X, Y, Z. Each element of a given list is an operation we can
perform. So e.g. in the GUI case above, all elements of X may be different
operations we can do on a text box. If we want to generate a bunch of unique
series of operations, it may look like:

        for x in X:
            for y in Y:
                for z in Z:
                    if (x, y, z) is a valid combination:
                        g = get_an_instance_of_my_GUI()
                        x(g)
                        y(g)
                        z(g)

The ideas of action, state, walk, etc. are drawn from this sort of thinking. An
Action instance is a member of one of the lists above, and the list is provided
by MyActionClass.get_option_set().

Given a specification of the state space and action space, it would be great to
generate tests that cover all the states x actions the GUI is capable of. And
when a new control is added, it would be great if our test generator
automatically generated additional cases. This lib provides a framework
for accomplishing that.

This lib additionally provides mechanisms for running a bunch of combtests in
parallel. An experienced tester may have sometimes heard feedback of the form
"we can't test all those combinations of X, we will have combinatorial
explosion." This can happen, but to get systematic coverage of the thing we are
testing, sometimes we really want to have a metric ton of combinations of
things we test. A way to alleviate that is through parallelism. This lib
provides a mechanism for dispatching the test cases for running across multiple
nodes + CPUs.  We will explore that in XXX.

