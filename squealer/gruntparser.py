
from java.util.regex import Pattern
from org.apache.pig.tools.grunt import GruntParser as BaseGruntParser

class GruntParser(BaseGruntParser):
    """
    Slightly modified GruntParser that accepts a list of aliases to override.

    This is a way to replace a pig query by another query.

    For example, if we have this map of overrides: Map<alias,query>;
      <A, A = LOAD '/path'> --> replace the alias A by A = LOAD '/path'</li>
      <DUMP, > --> remove the DUMP queries

    It might be possible to do the same thing in a less hacky way.
    e.g. pig.registerQuery replace the query of a certain alias...

    Note that this implementation is taken directly from PigUnit released
    with Pig 0.8
    """

    _AliasOverrides = None

    def __init__(self, stream, alias_overrides):
        BaseGruntParser.__init__(self, stream)
        self._AliasOverrides = alias_overrides

    def processPig(self, cmd):
        """Pig relations that have been blanked are dropped."""
        command = self.override(cmd)
        if command:
            BaseGruntParser.processPig(self, command)

    def override(self, query):
        """
        Overrides the relations of the pig script that we want to change.   
        new_query: The current pig query processed by the parser.
        return: The same query, or a modified query, or blank.
        """
        meta_data = {}
        for k,v in self._AliasOverrides.iteritems():
            self.saveLastStoreAlias(query, meta_data)
            if query.lower().startswith(k.lower() + " "):
                query = v
        self._AliasOverrides.update(meta_data)
        return query

    def saveLastStoreAlias(self, command, meta_data):
        """
        Saves the name of the alias of the last store.
        Maybe better to replace it by PigServer.getPigContext().getLastAlias().
        """
        if command.upper().startswith("STORE"):
            outputFile = Pattern.compile("STORE +([^']+) INTO.*", Pattern.CASE_INSENSITIVE)
            matcher = outputFile.matcher(command)
            if matcher.matches():
                meta_data["LAST_STORE_ALIAS"] = matcher.group(1)

