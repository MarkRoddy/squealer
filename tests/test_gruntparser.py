

import unittest
from  java.io import StringReader
from squealer.gruntparser import GruntParser


class TestPigServer(unittest.TestCase):

    parser = None
    override = None

    def setUp(self):
        self.override = {}
        self.override['STORE'] = ''
        self.override['DUMP'] = ''
        self.parser = GruntParser(StringReader(""), self.override)

    def testRemoveStores(self):
        self.assertEquals("", self.parser.override("STORE output INTO '/path';"))
        del self.override["STORE"]
        self.assertEquals("STORE output INTO '/path';", self.parser.override("STORE output INTO '/path';"))
    
    def testRemoveDumps(self):
        self.assertEquals("", self.parser.override("DUMP output;"))
        del self.override["DUMP"]
        self.assertEquals("DUMP output;", self.parser.override("DUMP output;"))

    def testReplaceLoad(self):
        self.override["A"] = "A = LOAD 'file';"
        self.assertEquals("A = LOAD 'file';",
                          self.parser.override("A = LOAD 'input.txt' AS (query:CHARARRAY);"))

    def testGetStoreAlias(self):
        del self.override["STORE"]
        self.parser.override("STORE output INTO '/path'")
        self.assertEquals("output", self.override.get("LAST_STORE_ALIAS"))

if __name__ == '__main__':
    unittest.main()
