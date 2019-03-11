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

Suppose one of us goes crazy and decides to write their own regex expression
parser. 

