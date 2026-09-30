import unittest
from pyspark.sql.types import IntegerType, StringType, DateType
from spark.schema import schema

class VerifyColumns(unittest.TestCase):

    def setUp(self):
        self.col_schema = schema()

    def test_verify_spark_columns(self):
        
        col_schema : list = [
        ["referrer", StringType()], 
        ["title", StringType()], 
        ["type", StringType()], 
        ["count", IntegerType()], 
        ["date_created", DateType()]]

        return self.assertEqual(col_schema, self.col_schema)

if __name__ == "__main__":
    unittest.main(argv=[""], verbosity=2, exit=True)