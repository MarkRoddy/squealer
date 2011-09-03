
import sys
import unittest
from squealer.pigproxy import PigProxy
from org.apache.log4j import LogManager, Level
from org.apache.pig.data import Tuple as PigTuple


class PigTest(unittest.TestCase):
    """
    A TestCase class with convience methods for unit testing
    pig scripts.

    Specify the path to your pig script in the 'PigScript' variable,
    and if you need to specify any arguments/properties to the script
    do so by setting the 'Parameters' attribute to dictionary with name/value
    pairs.
    """

    PigScript = ''
    Parameters = None

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        if not self.Parameters:
            self.Parameters = {}
        arglist = ["%s=%s" % (k, v) for (k, v) in self.Parameters.iteritems()]
        self._proxy = PigProxy.from_file(self.PigScript, arglist)

    def relation(self, alias):
        """
        Returns the data in the specified relation, converted to
        python types
        """
        output = []
        for t in self._proxy.get_alias(alias):
            output.append(self._to_python(t))
        return output

    def _to_python(self, value):
        """Converts a pig/java data type to its python equivilant"""
        if isinstance(value, PigTuple):
            return tuple([self._to_python(v) for v in value.getAll()])
        return value

    def override_data(self, alias, data):
        """Override the data in a relation with the records specified"""
        formatted_data = map(lambda t: '\t'.join(map(str,t)), data)
        self._proxy.override_to_data(alias, formatted_data)

    def override_query(self, alias, query):
        """
        Replaces the query of an aliases by another query.

        For example:
            B = FILTER A BY count > 5;
        overridden with:
            <B, B = FILTER A BY name == 'Pig';>
        becomes
            B = FILTER A BY name == 'Pig';

        alias: The alias to override.
        query: The new value of the alias.
        """
        self._proxy.override(alias, query)

    def assertSchemaEquals(self, alias, schema_string):
        """
        Assert that the specified relation has the supplied schema.
        schema_string should be a valid Pig schema notiation.
        """
        remove_whitespace = lambda s: ''.join(s.split())
        # Remove all white space
        schema_string = remove_whitespace(schema_string)
        if not schema_string.startswith('('):
            schema_string = '(' + schema_string
        if not schema_string.endswith(')'):
            schema_string += ')'
        actual_schema = self._proxy.schemaFor(alias)
        actual_schema = remove_whitespace(actual_schema)
        self.assertEqual(actual_schema, schema_string)
    
    def assertRelationEquals(self, alias, expected, ignore_ordering = True):
        """Assert that the specified alias has the expected set of records"""
        actual = self.relation(alias)
        if ignore_ordering:
            expected,actual = self.sortRelations(expected, actual)
        self.assertEqual(expected, actual)

    def assertLastStoreEquals(self, expected):
        """Assert that the alias in the last STORE operation of the script had the expected records"""
        alias = self._proxy.last_stored_alias_name()
        self.assertRelationEquals(alias, expected)

    def assertRelationAlmostEquals(self, alias, expected, places = 7, ignore_ordering = True):
        """
        Assert that the alias is equal the expected set of records except for in the
        case of floating point values.  These are compared for equality up to the
        specified number of decimal places.
        """
        actual = self.relation(alias)
        if len(actual) != len(expected):
            self.failRelationsNotEqual(expected, actual)

        if ignore_ordering:
            expected,actual = self.sortRelations(expected, actual)

        # Compare each tuple in expected and actual
        are_almost_equal = True
        for i,expected_tuple in enumerate(expected):
            actual_tuple = actual[i]
            if not len(actual_tuple) == len(expected_tuple):
                self.failRelationsNotEqual(expected,actual)

            # Compare expected and actual value in each tuple
            for j,expected_value in enumerate(expected_tuple):
                actual_value = actual_tuple[j]
                if isinstance(expected_value, float):
                    are_almost_equal &= self.almostEqual(expected_value, actual_value, places)
                else:
                    are_almost_equal &= (expected_value == actual_value)

        if not are_almost_equal:
            self.failRelationsNotEqual(expected, actual)

    def almostEqual(first, second, places):
        return round(abs(second-first), places) == 0
    almostEqual = staticmethod(almostEqual)
    
    def failRelationsNotEqual(self, first, second):
        """Fail the test because the two specified relations are not equal."""
        raise self.failureException, \
              '%s != %s' % (first, second)

    def sortRelations(self, *relations):
        sorted_relations = []
        for r in relations:
            r = list(r)
            r.sort()
            sorted_relations.append(r)
        return sorted_relations


class _TextTestResult(unittest._TextTestResult):

    def printErrorList(self, flavour, errors):
        for test, err in errors:
            self.stream.writeln(self.separator1)
            self.stream.writeln("%s: %s" % (flavour,self.getDescription(test)))
            if isinstance(test, PigTest):
                script = test._proxy.pig_script()
                if script:
                    self.stream.writeln("Script Run: %s" % script)
            self.stream.writeln(self.separator2)
            self.stream.writeln("%s" % err)


class TextTestRunner(unittest.TextTestRunner):

    def _makeResult(self):
        return _TextTestResult(self.stream, self.descriptions, self.verbosity)

class TestProgram(unittest.TestProgram):

    def parseArgs(self, argv):
        import getopt
        try:
            options, args = getopt.getopt(argv[1:], 'hHvq',
                                          ['help','verbose','quiet'])
            for opt, value in options:
                if opt in ('-h','-H','--help'):
                    self.usageExit()
                if opt in ('-q','--quiet'):
                    self.verbosity = 0
                if opt in ('-v','--verbose'):
                    self.verbosity = 2

            # Disable Logging unless verbose mode
            # explicitly requested
            if 2 != self.verbosity:
                self.silence_logs()

            if len(args) == 0 and self.defaultTest is None:
                self.test = self.testLoader.loadTestsFromModule(self.module)
                return
            if len(args) > 0:
                self.testNames = args
            else:
                self.testNames = (self.defaultTest,)
            self.createTests()
        except getopt.error, msg:
            self.usageExit(msg)

    def runTests(self):
        if self.testRunner is None:
            self.testRunner = TextTestRunner(verbosity=self.verbosity)
        result = self.testRunner.run(self.test)
        sys.exit(not result.wasSuccessful())

    def silence_logs(self):
        """Turn off logging messages in pig"""
        off = Level.toLevel("OFF")
        log = LogManager.getRootLogger()
        log.setLevel(off)

main = TestProgram
