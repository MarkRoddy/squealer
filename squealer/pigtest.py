
import sys
import unittest
from squealer.pigproxy import PigProxy
from org.apache.pig.data import Tuple as PigTuple


class PigTest(unittest.TestCase):
    """
    A TestCase class with convience methods for unit testing
    pig scripts.

    Specify the path to your pig script in the 'PigScript' variable,
    and if you need to specify any arguments/properties to the script
    do so by setting the 'Args' attribute to dictionary with name/value
    pairs.
    """

    PigScript = ''
    Args = None

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        if not self.Args:
            self.Args = {}
        arglist = ["%s=%s" % (k, v) for (k, v) in self.Args.iteritems()]
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
        self._proxy.override_to_data(alias, data)

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

    def assertRelationEquals(self, alias, expected):
        """Assert that the specified alias has the expected set of records"""
        actual = self.relation(alias)
        self.assertEqual(expected, actual)

    def assertLastStoreEquals(self, expected):
        """Assert that the alias in the last STORE operation of the script had the expected records"""
        alias = self._proxy.last_stored_alias_name()
        self.assertRelationEquals(alias, expected)

    def assertLastStoreEqualsFile(self, file_path, sep = ','):
        """Assert that the alias in the last STORE operation of the script is equal to data in file"""
        relation_name = self._proxy.last_stored_alias_name()
        actual_records = self.relation(relation_name)
        first_actual_record = actual_records[0]
        type_list = map(lambda x: x.__class__, first_actual_record)
        fobj = open(file_path)
        expected_records = []
        try:
            for r in fobj:
                str_vals = r.split(sep)
                casted_vals = []
                for i,v in enumerate(str_vals):
                    t = type_list[i]
                    casted_vals.append(t(v))
                casted_record = tuple(casted_vals)
                expected_records.append(casted_record)
        finally:
            fobj.close()
        self.assertEqual(expected_records, actual_records)

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

    def runTests(self):
        if self.testRunner is None:
            self.testRunner = TextTestRunner(verbosity=self.verbosity)
        result = self.testRunner.run(self.test)
        sys.exit(not result.wasSuccessful())

main = TestProgram
