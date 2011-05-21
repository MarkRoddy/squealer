
import unittest
from org.apache.hadoop.fs import Path
from squealer.cluster import Cluster
from squealer.pigproxy import PigProxy


class TestPigProxy(unittest.TestCase):

    proxy = None
    cluster = None

    PIG_SCRIPT = "pig-scripts/top_queries.pig"
    INPUT_FILE = "test-data/top_queries_input_data.txt"

    def assertOutput(self, proxy, alias, expected_list):
        proxy.register_script()
        self.assertEquals('\n'.join(expected_list), '\n'.join([str(i) for i in proxy.get_alias(alias)]))

    def assertLastOutput(self, proxy, expected_list):
        """
        Like assertOutput() but operates on the last STORE command
        """
        proxy.register_script()
        alias = proxy.alias_overrides["LAST_STORE_ALIAS"]
        self.assertOutput(proxy, alias, expected_list)

    def testNtoN(self):
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=top_3_queries",
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)
        output = [
            "(yahoo,25L)",
            "(facebook,15L)",
            "(twitter,7L)",
            ]
        self.assertOutput(proxy, "queries_limit", output)

    def testImplicitNtoN(self):
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=top_3_queries",
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)        
        output = [
            "(yahoo,25L)",
            "(facebook,15L)",
            "(twitter,7L)",
            ]
        self.assertLastOutput(proxy, output)

    def testTextInput(self):
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=top_3_queries",
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)
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
        "(yahoo,25L)",
        "(facebook,12L)",
        "(twitter,7L)",
            ]
        proxy.overrideToData("data", input_data)
        self.assertOutput(proxy, "queries_limit", output)

    def testSubset(self):
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=top_3_queries",
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)
        input_data = [
            "yahoo\t10",
            "twitter\t7",
            "facebook\t10",
            "yahoo\t15",
            "facebook\t5",
            "a\t1",
            "b\t2",
            "c\t3",
            "d\t4",
            "e\t5",
            ]
        output = [
            "(yahoo,25L)",
            "(facebook,15L)",
            "(twitter,7L)",
            ]
        proxy.overrideToData("data", input_data)
        self.assertOutput(proxy, "queries_limit", output);

    def testOverride(self):
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=top_3_queries",
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)
        proxy.override("queries_limit", "queries_limit = LIMIT queries_ordered 2;");
        output = [
            "(yahoo,25L)",
            "(facebook,15L)",
            ]
        self.assertLastOutput(proxy, output);

    def testInlinePigScript(self):
        script = '\n'.join([
            "data = LOAD '%s' AS (query:CHARARRAY, count:INT);" % self.INPUT_FILE,
            "queries_group = GROUP data BY query PARALLEL 1;",
            "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
            "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
            "queries_limit = LIMIT queries_ordered 3;",
            "STORE queries_limit INTO 'top_3_queries';",
            ])        
        proxy = PigProxy(script);
        output = [
            "(yahoo,25L)",
            "(facebook,15L)",
            "(twitter,7L)",
            ]
        self.assertLastOutput(proxy, output)

    def testGetLastAlias(self):
        script = '\n'.join([
            "data = LOAD '%s' AS (query:CHARARRAY, count:INT);" % self.INPUT_FILE,
            "queries_group = GROUP data BY query PARALLEL 1;",
            "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
            "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
            "queries_limit = LIMIT queries_ordered 3;",
            "STORE queries_limit INTO 'top_3_queries';",
            ])
        proxy = PigProxy(script)
        expected = \
        "(yahoo,25L)\n" + \
        "(facebook,15L)\n" + \
        "(twitter,7L)"
        self.assertEquals(expected, '\n'.join([str(i) for i in proxy.get_alias("queries_limit")]))

    def testWithUdf(self):
        script = '\n'.join([
            # "REGISTER myIfNeeded.jar;",
            "DEFINE TOKENIZE TOKENIZE();",
            "data = LOAD '%s' AS (query:CHARARRAY, count:INT);" % self.INPUT_FILE,
            "queries = FOREACH data GENERATE query, TOKENIZE(query) AS query_tokens;",
            "queries_ordered = ORDER queries BY query DESC PARALLEL 1;",
            "queries_limit = LIMIT queries_ordered 3;",
            "STORE queries_limit INTO 'top_3_queries';",
            ])
        proxy = PigProxy(script);
        output = [
            "(yahoo,{(yahoo)})",
            "(yahoo,{(yahoo)})",
            "(twitter,{(twitter)})",
            ]
        self.assertLastOutput(proxy, output);

    def testArgFiles(self):
        argsFile = [
            "test-data/top_queries_params.txt"
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, arg_files = argsFile)
        output = [
            "(yahoo,25L)",
            "(facebook,15L)",
            "(twitter,7L)",
            ]
        self.assertOutput(proxy, "queries_limit", output)

    def testStore(self):
        from tempfile import mktemp
        tempdir = mktemp()
        outfile = tempdir + '/top_3_queries'
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=" + outfile,
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)

        # By default all STORE and DUMP commands are removed
        proxy.unoverride("STORE")
        proxy.run_script()
        cluster = Cluster(proxy.pig.getPigContext())
        self.assert_(cluster.delete(Path(outfile)))

    def testLastStoreName(self):
        args = [
            "n=3",
            "reducers=1",
            "input=" + self.INPUT_FILE,
            "output=top_3_queries",
            ]
        proxy = PigProxy.from_file(self.PIG_SCRIPT, args)
        self.assertEqual("queries_limit", proxy.last_stored_alias_name())

if __name__ == '__main__':
    unittest.main()
