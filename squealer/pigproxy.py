
from squealer.pigserver import PigServer
from squealer.cluster import Cluster


from java.io import BufferedReader, StringReader, StringWriter, File, PrintWriter

from org.apache.pig.tools.parameters import ParameterSubstitutionPreprocessor
from java.lang import System


from org.apache.pig import ExecType
EXEC_CLUSTER = "pigunit.exectype.cluster"

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
        self.assertEquals('\n'.join(expected_list), '\n'.join(self.get_alias(alias)))

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
            if (System.getProperties().containsKey(EXEC_CLUSTER)):
                self.pig = PigServer(ExecType.MAPREDUCE)
            else:
                self.pig = PigServer(ExecType.LOCAL)

            self.cluster = Cluster(self.pig.getPigContext())
        return self.cluster

    def get_alias(self, alias):
        self.register_script()
        return iter(self.pig.openIterator(alias))
