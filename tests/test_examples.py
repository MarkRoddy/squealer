"""
The ExampleTest class in this module documents both how to define
a test case for a Pig script, and common tasks you might like
to perform in the course of writing tests.

In order to create your own you'll need to define a class that
inherits from squealer.PigTest (just like python's
unittest.TestCase), specify the path to the script you're testing,
and start writing tests.

The squealer specific main() method is optional as PigTest cases
will work with unittest's main().  However, it does include several
nice features that make running test cases more enjoyable.  Also,
the squealer main() will also run traditional unittest.TestCase
based tests.
"""

from squealer import PigTest, main


class ExampleTest(PigTest):
    
    # Specify the path to the pig script that you're testing, this
    # can be either an absolute path (if you hate your coworkers) or
    # a path relative to where you'll be running your tests.
    PigScript = "tests/pig-scripts/top_queries.pig"

    # There are several values you can specify vai the Args dictionary,
    # though the most common ones are parameters.  In this case the
    # pig script under test uses parameters $input and $output.  This
    # parameter is completely optional though.  
    Args = {
        "input" : "tests/data/top_queries_input_data.txt",
        "output" : "top_3_queries",
        }
    
    def testSimpleAssertionOfARelationsRecords(self):
        """
        Assert the expected set of records for one of the
        relations in the pig script.
        """

        # Pig relations contain an interable of tuple objects,
        # one for each record in the relation.  To construct
        # the expected set of records, create a list of tuples
        # with the expected data for each record.
        output = [
            ('yahoo',25),
            ('facebook',15),
            ('twitter',7),
            ]

        # To perform the actual assertion specify the name of the
        # relation and pass in the expected records
        self.assertRelationEquals("queries_limit", output)

    def testOverrideLoadCommandWithSpecifiedData(self):
        """Explicitly specify the set of records a relation should have"""
        # In order to thoroughly test a pig script you will likely have to
        # specify a variety of inputs to test different code paths. You
        # could specify a different input file for each of these cases,
        # but in practice, having the data within the fixture for the
        # condition being tested is much easier to understand and debug
        # than having to jump back and forth between different input files.
        input_data = [
            ("yahoo", 10),
            ("twitter", 7),
            ("facebook", 10),
            ("yahoo", 15),
            ("facebook", 2),
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 4),
            ("e", 5),
            ]
        self.override_data("data", input_data)

        output = [
            ("yahoo",25),
            ("facebook",12),
            ("twitter",7),
            ]
        self.assertRelationEquals("queries_limit", output)


if __name__ == '__main__':
    main()
