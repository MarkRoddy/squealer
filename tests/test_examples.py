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

    # To specify parameters for the pig script, create a class
    # attribute named 'Parameters' set to a dictionary containing
    # the parameter names/values you wish to specify. This attribute
    # is not necessary if your script does not have any parameters
    Parameters = {
        "input" : "tests/data/top_queries_input_data.txt",
        "output" : "top_3_queries",
        }
    
    def testSimpleAssertionOfARelationsRecords(self):
        """Assert the expected set of records for a relation"""
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

    def testIgnoringOrderingOfRecords(self):
        """
        Assert the expected set of records for a relation, ignoring
        the order in which they appear.
        """
        # For certain Pig operations, it can be difficult to determine
        # the order in which the records will be in for a relation. For
        # instance if a group and then aggregate function is performed.
        # However, in many of these cases you may not care about the
        # ordering. To deal with this, all equality assertions take an
        # optional argument `ignore_ordering`.  If False a strict
        # comparison is performed taking into account the order in which
        # the records appear, otherwise if True, the order of the expected
        # records in the relation are ignored.
        # Note that the default value for ignore_ordering is True so all
        # comparisons will ignore the ordering unless explicitly specified
        # not to.

        # Actual records in the pig script:
        # ('yahoo',25)
        # ('facebook',15)
        # ('twitter',7)
        output = [
            ('yahoo',25),
            ('twitter',7), 
            ('facebook',15),
            ]        
        self.assertRelationEquals("queries_limit", output, ignore_ordering = True)

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

    def testOverrideQueryUsed(self):
        """Override the query/command used to generate data in a relation"""
        # In addtion to being able to override a relation with a specific set
        # of records, you can also override the query that generates that
        # relation. This is especially useful when the data you want in the 
        # relation can be easier expressed as a function of the input rather than
        # hard coding of the actual records. You can also think of this as
        # a method of 'mocking' the pig script in order to test a single portion
        # of it.

        # To override the relation with a new command specify the name of the relation
        # you are overriding as well as the command to be substituted. Note that no
        # checking is performed on the command specified so it is up to you to ensure
        # that the overriden command is valid in the context of the script being tested.
        self.override_query("queries_limit", "queries_limit = LIMIT queries_ordered 2;");

        output = [
            ('yahoo',25),
            ('facebook',15),
            ]
        self.assertLastStoreEquals(output)


class TestPigScriptWithFloatingPointOperations(PigTest):
    """
    In addition to doing strict comparisons of data, Squealer
    also supports performing approximations of equality for
    floating point numbers.  This is particularly useful if
    you are doing a large amount of floating point arithmetic
    whose exact results are dependent on the floating point
    implementation of the particular system (in the case of
    Pig, this would be the JVM).  Using aproximation is much
    easier than attempting to determine the exact resulting
    value, especially for complex operations.
    """
    
    PigScript = "tests/pig-scripts/top_queries_floating_point.pig"

    Parameters = {
        "input" : "tests/data/top_queries_input_data.txt",
        "output" : "top_3_queries",
        }

    def testRelationWithFloatingPointValues(self):
        input_data = [
            ("yahoo", 10.000001),
            ("yahoo", 15.000003),
            ("facebook", 2.000001),
            ]
        self.override_data("data", input_data)
        
        output = [
            ('yahoo',25.000002), # Off by 0.000002
            ('facebook',2.0000008), # Off by 0.000007
            ]
        # To perform an exproximate comparison for equality call
        # the assertRelationAlmostEquals() method instead of 
        # assertRelationEquals(), passing in the number of digits
        # of precision to be used in the comparison.  For non-float
        # values a strict equality operation is performed.
        self.assertRelationAlmostEquals("queries_sum", output, places = 4);



if __name__ == '__main__':
    main()
