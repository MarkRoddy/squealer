
import unittest
from squealer.pigtest import PigTest

class TestPigTest(unittest.TestCase):

    PIG_SCRIPT = "tests/pig-scripts/top_queries.pig"
    INPUT_FILE = "tests/data/top_queries_input_data.txt"

    def testNtoN(self):
        class some_test(PigTest):
            Args = {
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
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testTextInput(self):
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
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testSubset(self):
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


if __name__ == '__main__':
    unittest.main()

