from pyspark.sql import SparkSession
from pyspark.sql.functions import split, current_date
import os
from pyspark.sql.functions import col
from schema import schema
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

class SparkKafkaConsumer():
    def __init__(self):
        self.kafka_bootstrap_servers = "localhost:9092"
        self.kafka_topic = "clickstream_data"
        self.spark = SparkSession.builder \
            .appName("KafkaStreamProcessing") \
            .config("spark.jars", "./postgresql-42.7.2.jar") \
            .enableHiveSupport() \
            .getOrCreate()
            
    def read_kafka_topic(self):
        # Read data from Kafka topic
        df = self.spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
        .option("subscribe", self.kafka_topic) \
        .load()

        # Perform transformations or processing
        processed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .withColumn("value", split("value", "\t")) \
            .selectExpr("value[0] AS {0}".format(schema()[0][0]), 
                        "value[1] AS {0}".format(schema()[1][0]),
                        "value[2] AS {0}".format(schema()[2][0]),
                        "value[3] AS {0}".format(schema()[3][0])
                        )

        processed_df = processed_df.fillna("NA", subset=["title", "referrer", "type"])
        processed_df = processed_df.withColumn(schema()[4][0], current_date())
        processed_df = processed_df.withColumn(schema()[0][0], col(schema()[0][0]).cast(schema()[0][1]))
        processed_df = processed_df.withColumn(schema()[1][0], col(schema()[1][0]).cast(schema()[1][1]))
        processed_df = processed_df.withColumn(schema()[2][0], col(schema()[2][0]).cast(schema()[2][1]))
        processed_df = processed_df.withColumn(schema()[3][0], col(schema()[3][0]).cast(schema()[3][1]))
        processed_df = processed_df.withColumn(schema()[4][0], col(schema()[4][0]).cast(schema()[4][1]))

        partitioned_clustered_df = processed_df \
            .repartition(schema()[1][0])
        writing_sink = partitioned_clustered_df.writeStream \
            .trigger(processingTime='59 seconds') \
            .foreachBatch(self.foreach_function) \
            .start()
        writing_sink.awaitTermination()

    def foreach_function(self, df, epoch_id):
        df.printSchema()
        df.show()
        df.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "tz_table") \
        .option("user", "you") \
        .option("password", "you.com") \
        .option("truncate", False) \
        .save()

consumer=SparkKafkaConsumer()
consumer.read_kafka_topic()
