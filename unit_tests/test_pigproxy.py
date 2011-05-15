
import os
import unittest

from org.apache.hadoop.fs import Path

from squealer.pigproxy import PigProxy


class TestPigProxy(unittest.TestCase):

    proxy = None
    cluster = None

    PIG_SCRIPT = "pig-scripts/top_queries.pig"

#     def setUp(self):
#         self.cluster = PigProxy.getCluster()
#         self.cluster.update(
#             Path("test/data/pigunit/top_queries_input_data.txt"),
#             Path("top_queries_input_data.txt"))

    def testNtoN(self):
        args = [
            "n=3",
            "reducers=1",
            "input=top_queries_input_data.txt",
            "output=top_3_queries",
            ]
        proxy = PigProxy.fromFile(self.PIG_SCRIPT, args)
        output = [
            "(yahoo,25)",
            "(facebook,15)",
            "(twitter,7)",
            ]
        proxy.assertOutput("queries_limit", output)


#   @Test
#   public void testImplicitNtoN() throws ParseException, IOException {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };
#     test = new PigTest(PIG_SCRIPT, args);

#     String[] output = {
#         "(yahoo,25)",
#         "(facebook,15)",
#         "(twitter,7)",
#     };

#     test.assertOutput(output);
#   }

#   @Test
#   public void testTextInput() throws ParseException, IOException  {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };
#     test = new PigTest(PIG_SCRIPT, args);

#     String[] input = {
#         "yahoo\t10",
#         "twitter\t7",
#         "facebook\t10",
#         "yahoo\t15",
#         "facebook\t5",
#         "a\t1",
#         "b\t2",
#         "c\t3",
#         "d\t4",
#         "e\t5",
#     };

#     String[] output = {
#         "(yahoo,25)",
#         "(facebook,15)",
#         "(twitter,7)",
#     };

#     test.assertOutput("data", input, "queries_limit", output);
#   }

#   @Test
#   public void testSubset() throws ParseException, IOException  {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };
#     test = new PigTest(PIG_SCRIPT, args);

#     String[] input = {
#         "yahoo\t10",
#         "twitter\t7",
#         "facebook\t10",
#         "yahoo\t15",
#         "facebook\t5",
#         "a\t1",
#         "b\t2",
#         "c\t3",
#         "d\t4",
#         "e\t5",
#     };

#     String[] output = {
#         "(yahoo,25)",
#         "(facebook,15)",
#         "(twitter,7)",
#     };

#     test.assertOutput("data", input, "queries_limit", output);
#   }

#   @Test
#   public void testOverride() throws ParseException, IOException  {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };
#     test = new PigTest(PIG_SCRIPT, args);

#     test.override("queries_limit", "queries_limit = LIMIT queries_ordered 2;");

#     String[] output = {
#         "(yahoo,25)",
#         "(facebook,15)",
#     };

#     test.assertOutput(output);
#   }

#   @Test
#   public void testInlinePigScript() throws ParseException, IOException  {
#     String[] script = {
#         "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
#         "queries_group = GROUP data BY query PARALLEL 1;",
#         "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
#         "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
#         "queries_limit = LIMIT queries_ordered 3;",
#         "STORE queries_limit INTO 'top_3_queries';",
#     };

#     test = new PigTest(script);

#     String[] output = {
#         "(yahoo,25)",
#         "(facebook,15)",
#         "(twitter,7)",
#     };

#     test.assertOutput(output);
#   }

#   @Test
#   public void testFileOutput() throws ParseException, IOException {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };
#     test = new PigTest(PIG_SCRIPT, args);

#     test.assertOutput(new File("test/data/pigunit/top_queries_expected_top_3.txt"));
#   }

#   @Test
#   public void testArgFiles() throws ParseException, IOException {
#     String[] argsFile = {
#         "test/data/pigunit/top_queries_params.txt"
#     };

#     test = new PigTest(PIG_SCRIPT, null, argsFile);

#     test.assertOutput(new File("test/data/pigunit/top_queries_expected_top_3.txt"));
#   }

#   @Test
#   public void testGetLastAlias() throws ParseException, IOException  {
#     String[] script = {
#         "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
#         "queries_group = GROUP data BY query PARALLEL 1;",
#         "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
#         "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
#         "queries_limit = LIMIT queries_ordered 3;",
#         "STORE queries_limit INTO 'top_3_queries';",
#     };

#     test = new PigTest(script);

#     String expected =
#         "(yahoo,25)\n" +
#         "(facebook,15)\n" +
#         "(twitter,7)";

#     TestCase.assertEquals(expected, StringUtils.join(test.getAlias("queries_limit"), "\n"));
#   }

#   @Test
#   public void testWithUdf() throws ParseException, IOException  {
#     String[] script = {
#      // "REGISTER myIfNeeded.jar;",
#         "DEFINE TOKENIZE TOKENIZE();",
#         "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
#         "queries = FOREACH data GENERATE query, TOKENIZE(query) AS query_tokens;",
#         "queries_ordered = ORDER queries BY query DESC PARALLEL 1;",
#         "queries_limit = LIMIT queries_ordered 3;",
#         "STORE queries_limit INTO 'top_3_queries';",
#     };

#     test = new PigTest(script);

#     String[] output = {
#         "(yahoo,{(yahoo)})",
#         "(yahoo,{(yahoo)})",
#         "(twitter,{(twitter)})",
#     };

#     test.assertOutput(output);
#   }

#   @Test
#   public void testStore() throws ParseException, IOException {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };
#     test = new PigTest(PIG_SCRIPT, args);

#     // By default PigUnit removes all the STORE and DUMP
#     test.unoverride("STORE");

#     test.runScript();

#     TestCase.assertTrue(cluster.delete(new Path("top_3_queries")));
#   }

#   @Ignore("Not ready yet")
#   @Test
#   public void testWithMock() throws ParseException, IOException {
#     String[] args = {
#         "n=3",
#         "reducers=1",
#         "input=top_queries_input_data.txt",
#         "output=top_3_queries",
#         };

#     PigServer mockServer = null;
#     Cluster mockCluster = null;

#     test = new PigTest(PIG_SCRIPT, args, mockServer, mockCluster);

#     test.assertOutput(new File("data/top_queries_expected_top_3.txt"));
#   }
# }


if __name__ == '__main__':
    unittest.main()
