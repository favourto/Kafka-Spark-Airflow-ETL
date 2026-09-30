from pyspark.sql.types import IntegerType, StringType, DateType

def schema():
    columns : list= [
        ["referrer", StringType()], 
        ["title", StringType()], 
        ["type", StringType()], 
        ["count", IntegerType()], 
        ["date_created", DateType()]]
    return columns