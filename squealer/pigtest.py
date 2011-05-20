
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
        arglist = ["%s=%s"%(k,v) for (k,v) in self.Args.iteritems()]
        self._proxy = PigProxy.fromFile(self.PigScript, arglist)

    def get_relation(self, alias):
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
        
    def assertRelationEquals(self, alias, expected):
        """Assert that the specified alias has the expected set of records"""
        actual = self.get_relation(alias)
        self.assertEqual(expected, actual)

    def assertLastStoreEquals(self, expected):
        """Assert that the alias in the last STORE operation of the script had the expected records"""
        self._proxy.register_script()
        alias = self._proxy.alias_overrides["LAST_STORE_ALIAS"]
        self.assertRelationEquals(alias, expected)

    def override_data(self, alias, data):
        """Override the data in a relation with that specified"""
        self._proxy.overrideToData(alias, data)

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
