
import unittest
from squealer.pigtest import PigTest


class TestPigTest(unittest.TestCase):

    PIG_SCRIPT = "pig-scripts/top_queries.pig"
    INPUT_FILE = "test-data/top_queries_input_data.txt"

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
            


#     def testFileOutput(self):
#         args = [
#             "n=3",
#             "reducers=1",
#             "input=" + self.INPUT_FILE,
#             "output=top_3_queries",
#             ]
#         test = PigProxy.fromFile(self.PIG_SCRIPT, args)
#         fobj = open("test-data/top_queries_expected_top_3.txt")
#         try:
#             test.assertOutputEqualsFile(fobj)
#         finally:
#             fobj.close()

#     def testGetLastAlias(self):
#         script = '\n'.join([
#             "data = LOAD '%s' AS (query:CHARARRAY, count:INT);" % self.INPUT_FILE,
#             "queries_group = GROUP data BY query PARALLEL 1;",
#             "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
#             "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
#             "queries_limit = LIMIT queries_ordered 3;",
#             "STORE queries_limit INTO 'top_3_queries';",
#             ])
#         test = PigProxy(script)
#         expected = \
#         "(yahoo,25L)\n" + \
#         "(facebook,15L)\n" + \
#         "(twitter,7L)"
#         self.assertEquals(expected, '\n'.join([str(i) for i in test.get_alias("queries_limit")]))

#     def testWithUdf(self):
#         script = '\n'.join([
#             # "REGISTER myIfNeeded.jar;",
#             "DEFINE TOKENIZE TOKENIZE();",
#             "data = LOAD '%s' AS (query:CHARARRAY, count:INT);" % self.INPUT_FILE,
#             "queries = FOREACH data GENERATE query, TOKENIZE(query) AS query_tokens;",
#             "queries_ordered = ORDER queries BY query DESC PARALLEL 1;",
#             "queries_limit = LIMIT queries_ordered 3;",
#             "STORE queries_limit INTO 'top_3_queries';",
#             ])
#         test = PigProxy(script);
#         output = [
#             "(yahoo,{(yahoo)})",
#             "(yahoo,{(yahoo)})",
#             "(twitter,{(twitter)})",
#             ]
#         test.assertLastOutput(output);



#     def testArgFiles(self):
#         argsFile = [
#             "test-data/top_queries_params.txt"
#             ]
#         test = PigProxy.fromFile(self.PIG_SCRIPT, arg_files = argsFile)
#         fobj = open("test-data/top_queries_expected_top_3.txt")
#         try:
#             test.assertOutputEqualsFile(fobj)
#         finally:
#             fobj.close()



#     def testStore(self):
#         args = [
#             "n=3",
#             "reducers=1",
#             "input=" + self.INPUT_FILE,
#             "output=top_3_queries",
#             ]
#         test = PigProxy.fromFile(self.PIG_SCRIPT, args)

#         # By default all STORE and DUMP commands are removed
#         test.unoverride("STORE")
#         test.run_script()
#         self.assertTrue(test.cluster.delete(Path("top_3_queries")))

if __name__ == '__main__':
    unittest.main()

