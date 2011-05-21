

from squealer.cluster import Cluster
from squealer.pigserver import PigServer

from java.lang import System, StringBuilder
from java.io import BufferedReader, StringReader, StringWriter, File, PrintWriter

from org.apache.pig import ExecType
from org.apache.pig.data import DataType
from org.apache.pig.impl.logicalLayer.schema import Schema
from org.apache.pig.tools.parameters import ParameterSubstitutionPreprocessor


class PigProxy(object):
    """Functionality for interacting with the pig interpreter"""
    
    orig_pig_code = None
    args = None
    arg_files = None
    alias_overrides = None    
    cluster = None
    
    def __init__(self, pig_code, args = None, arg_files = None):
        """
        pig_code: The text of the Pig script to test with no substitution or change
        args: The list of arguments of the script.
        arg_files: The list of file arguments of the script.
        """
        self.orig_pig_code = pig_code
        if args:
            self.args = args
        else:
            self.args = []
        if arg_files:
            self.arg_files = arg_files
        else:
            self.arg_files = []
        self.alias_overrides = {
            "STORE" : "",
            "DUMP" : "",
            }

    def fromFile(cls, pig_script, args = None, arg_files = None):
        f = open(pig_script, 'r')
        try:
            pig_code = f.read()
        finally:
            f.close()
        return cls(pig_code, args, arg_files)
    fromFile = classmethod(fromFile)

    def assertOutput(self, alias, expected_list):
        self.register_script()
        self.assertEquals('\n'.join(expected_list), '\n'.join([str(i) for i in self.get_alias(alias)]))

    def assertLastOutput(self, expected_list):
        """
        Like assertOutput() but operates on the last STORE command
        """
        self.register_script()
        alias = self.alias_overrides["LAST_STORE_ALIAS"]
        self.assertOutput(alias, expected_list)

    def assertEquals(self, expected, actual):
        if not expected == actual:
            raise AssertionError("""
Expected Output:
%s

Actual Output:
%s
""" % (expected, actual))

    def register_script(self):
        """
        Registers a pig scripts with its variables substituted.
        raises: IOException If a temp file containing the pig script could not be created.
        raises: ParseException The pig script could not have all its variables substituted.

        todo: Refactor this processes that result in calling this method.  This method gets
        called twice for a single assert as every method that needs the data assumes no one
        else has called it (even methods that call other methods that call it (assertOutput()
        calls get_alias() which both call this method).  
        """
        self.getCluster()

        pigIStream = BufferedReader(StringReader(self.orig_pig_code))
        pigOStream =  StringWriter()

        ps = ParameterSubstitutionPreprocessor(50) # Where does 50 come from?
        ps.genSubstitutedFile(pigIStream, pigOStream, self.args, self.arg_files)

        substitutedPig = pigOStream.toString()
        f = File.createTempFile("tmp", "pigunit")
        pw = PrintWriter(f)
        pw.println(substitutedPig)
        pw.close()

        pigSubstitutedFile = f.getCanonicalPath()
        print "Running: " + pigSubstitutedFile

        self.pig.registerScript(pigSubstitutedFile, self.alias_overrides)

    def getCluster(self):
        """
        Connects and starts if needed the PigServer.
        return: The cluster where input files can be copied.

        todo: this code is directly translated from the Java PigUnit implementation, and is
        a major structural limitation as it forces there to be a single PigServer per
        instance (which has some side affects that are not readily apparent). This may or may
        not be what the user desires.  At the very least the consumer should be able to
        specify if they want this behavior or not.  This will be addressed in the future,
        but right now I'm trying to get this out the door.
        """
        if not self.cluster:
            if (System.getProperties().containsKey("pigunit.exectype.cluster")):
                self.pig = PigServer(ExecType.MAPREDUCE)
            else:
                self.pig = PigServer(ExecType.LOCAL)

            self.cluster = Cluster(self.pig.getPigContext())
        return self.cluster

    def run_script(self):
        self.register_script()

    def get_alias(self, alias):
        self.register_script()
        return iter(self.pig.openIterator(alias))

    def override(self, alias, query):
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
        self.alias_overrides[alias] = query

    def unoverride(self, alias):
        """Remove an override placed on an alias"""
        if alias in self.alias_overrides:
            del self.alias_overrides[alias]        

    def overrideToData(self, alias, input_data):
        """
        Override a statement so that the alias results in having the
        specified set of data
        """
        self.register_script()
        sb = []
        sb = StringBuilder()
        Schema.stringifySchema(sb, self.pig.dumpSchema(alias), DataType.TUPLE)
        
        destination = "pigunit-input-overriden.txt"
        self.cluster.copyContentFromLocalFile(input_data, destination, True)
        self.override(alias, "%s = LOAD '%s' AS %s;" % (alias, destination, sb.toString()))

