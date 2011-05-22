
import unittest
from squealer.pigtest import PigTest

class TestPigTest(unittest.TestCase):

    PIG_SCRIPT = "tests/pig-scripts/top_queries.pig"
    INPUT_FILE = "tests/data/top_queries_input_data.txt"

    def testNtoN(self):
        class some_test(PigTest):
            Args = {
                "n" : 3,
                "reducers" : 1,
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testNoToN(self):
                output = [
                    ('yahoo',25),
                    ('facebook',15),
                    ('twitter',7),
                    ]
                self.assertRelationEquals("queries_limit", output)
        test = some_test('testNoToN')
        test.testNoToN()

    def testImplicitNtoN(self):
        class some_test(PigTest):
            Args = {
                "n" : 3,
                "reducers" : 1,
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testImplicitNtoN(self):
                output = [
                    ('yahoo',25),
                    ('facebook',15),
                    ('twitter',7),
                    ]
                test.assertLastStoreEquals(output)
        test = some_test('testImplicitNtoN')
        test.testImplicitNtoN()

    def testTextInput(self):
        class some_test(PigTest):
            Args = {
                "n" : 3,
                "reducers" : 1,
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testTextInput(self):
                input_data = [
                    "yahoo\t10",
                    "twitter\t7",
                    "facebook\t10",
                    "yahoo\t15",
                    "facebook\t2",
                    "a\t1",
                    "b\t2",
                    "c\t3",
                    "d\t4",
                    "e\t5",
                    ]
                output = [
                    ("yahoo",25),
                    ("facebook",12),
                    ("twitter",7),
                    ]
                self.override_data("data", input_data)
                self.assertRelationEquals("queries_limit", output)
        test = some_test('testTextInput')
        test.testTextInput()

    def testSubset(self):
        class some_test(PigTest):
            Args = {
                "n" : 3,
                "reducers" : 1,
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testSubset(self):
                input_data = [
                    "yahoo\t10",
                    "twitter\t7",
                    "facebook\t10",
                    "yahoo\t15",
                    "facebook\t2",
                    "a\t1",
                    "b\t2",
                    "c\t3",
                    "d\t4",
                    "e\t5",
                    ]
                output = [
                    ('yahoo',25),
                    ('facebook',12),
                    ('twitter',7),
                    ]
                self.override_data("data", input_data)
                self.assertRelationEquals("queries_limit", output);
        test = some_test('testSubset')
        test.testSubset()

    def testOverride(self):
        class some_test(PigTest):
            Args = {
                "n" : 3,
                "reducers" : 1,
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testOverride(self):
                self.override_query("queries_limit", "queries_limit = LIMIT queries_ordered 2;");
                output = [
                    ('yahoo',25),
                    ('facebook',15),
                    ]
                self.assertLastStoreEquals(output)
        test = some_test('testOverride')
        test.testOverride()
            
    def testFileOutput(self):
        class some_test(PigTest):
            Args = {
                "n" : 3,
                "reducers" : 1,
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testFileOutput(self):
                fobj = open("tests/data/top_queries_expected_top_3.txt")
                try:
                    self.assertLastStoreEqualsFile(fobj)
                finally:
                    fobj.close()
        test = some_test('testFileOutput')
        test.testFileOutput()


if __name__ == '__main__':
    unittest.main()

