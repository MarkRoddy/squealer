
from squealer.gruntparser import GruntParser

from java.io import File
from java.io import FileReader

from org.apache.pig import PigServer as BasePigServer


class PigServer(BasePigServer):
    """
    Slightly modified PigServer that accepts a list of Pig aliases to override.
    The list is given to the GruntParser.

    Note that this implementation is taken directly from PigUnit released
    with Pig 0.8
    """

    def __init__(self, exec_type, properties = None):
        if properties:
            BasePigServer.__init__(self, exec_type, properties)
        else:
            BasePigServer.__init__(self, exec_type)

    def registerScript(self, file_name, alias_overrides):
        """
        Parses and registers the pig script.
        file_name:  The Pig script file.
        alias_overrides: The list of aliases to override in the Pig script.
        """
        grunt = GruntParser(FileReader(File(file_name)), alias_overrides)
        grunt.setInteractive(False)
        grunt.setParams(self)
        grunt.parseStopOnError(True)
