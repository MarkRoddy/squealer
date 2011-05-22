
import unittest
from squealer.pigtest import PigTest

class TestPigTest(unittest.TestCase):

    PIG_SCRIPT = "tests/pig-scripts/top_queries.pig"
    INPUT_FILE = "tests/data/top_queries_input_data.txt"

    def testNtoN(self):
        class some_test(PigTest):
            Parameters = {
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

    def testNtoNWithDifferentOrdering_OrderingDoesntMatter(self):
        class some_test(PigTest):
            Parameters = {
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testNtoNWithDifferentOrdering_OrderingDoesntMatter(self):
                output = [
                    ('yahoo',25),
                    ('twitter',7),
                    ('facebook',15),
                    ]
                self.assertRelationEquals("queries_limit", output)
        test = some_test('testNtoNWithDifferentOrdering_OrderingDoesntMatter')
        test.testNtoNWithDifferentOrdering_OrderingDoesntMatter()

    def testNtoNWithDifferentOrdering_OrderingDoesMatter(self):
        class some_test(PigTest):
            Parameters = {
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testNtoNWithDifferentOrdering_OrderingDoesMatter(self):
                output = [
                    ('yahoo',25),
                    ('twitter',7),
                    ('facebook',15),
                    ]
                self.assertRelationEquals("queries_limit", output, order_matters = True)
        test = some_test('testNtoNWithDifferentOrdering_OrderingDoesMatter')
        self.assertRaises(self.failureException, test.testNtoNWithDifferentOrdering_OrderingDoesMatter)

    def testImplicitNtoN(self):
        class some_test(PigTest):
            Parameters = {
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
            Parameters = {
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
            Parameters = {
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
            Parameters = {
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

class TestPigFloatingPointTest(unittest.TestCase):

    PIG_SCRIPT = "tests/pig-scripts/top_queries_floating_point.pig"
    INPUT_FILE = "tests/data/top_queries_input_data.txt"

    def test_almostEqual_AreAlmostEqual(self):       
        self.assert_(PigTest.almostEqual(25.000002, 25.000003814697266, 4))

    def test_almostEqual_NotAlmostEqual_IntegerPart(self):
        self.assert_(not PigTest.almostEqual(28.000002, 25.000003814697266, 4))

    def test_almostEqual_NotAlmostEqual_TooManyPointsApart(self):
        self.assert_(not PigTest.almostEqual(28.00001, 25.000003814697266, 4))

    def testRelationAlmostequals_AreAlmostEqual(self):
        class some_test(PigTest):
            Parameters = {
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testRelationAlmostequals(self):
                input_data = [
                    ("yahoo", 10.000001),
                    ("yahoo", 15.000003),
                    ("facebook", 2.000001),
                    ]
                output = [
                    ('yahoo',25.000002),
                    ('facebook',2.0000008),
                    ]
                self.override_data("data", input_data)
                self.assertRelationAlmostEquals("queries_sum", output, 4);
        test = some_test('testRelationAlmostequals')
        test.testRelationAlmostequals()

    def testRelationAlmostequals_AreNotAlmostEqual(self):
        class some_test(PigTest):
            Parameters = {
                "input" : self.INPUT_FILE,
                "output" : "top_3_queries",
                }
            PigScript = self.PIG_SCRIPT
            def testRelationAlmostequals_AreNotAlmostEqual(self):
                input_data = [
                    ("yahoo", 10.000001),
                    ("yahoo", 15.000003),
                    ("facebook", 2.000001),
                    ]
                output = [
                    ('yahoo',25.000002),
                    ('facebook',2.04),
                    ]
                self.override_data("data", input_data)
                self.assertRaises(self.failureException, self.assertRelationAlmostEquals, "queries_sum", output)
        test = some_test('testRelationAlmostequals_AreNotAlmostEqual')
        test.testRelationAlmostequals_AreNotAlmostEqual()


if __name__ == '__main__':
    unittest.main()

