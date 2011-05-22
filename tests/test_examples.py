

from squealer import PigTest, main



class ExampleTest(PigTest):

    PigScript = "tests/pig-scripts/top_queries.pig"
    
    Args = {
        "n" : 3,
        "reducers" : 1,
        "input" : "tests/data/top_queries_input_data.txt",
        "output" : "top_3_queries",
        }
    
    def testNoToN(self):
        output = [
            ('yahoo',25),
            ('facebook',15),
            ('twitter',7),
            ]
        self.assertRelationEquals("queries_limit", output)


if __name__ == '__main__':
    main()
