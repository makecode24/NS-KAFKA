import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, lit

from util import initialize_spark_session

class SparkConsumer:
    def __init__(self,) -> None:
        logging.basicConfig(level=logging.INFO)
        with open("streaming_cofig.yaml", "r") as yamlfile:
            config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        self.config = config

        self.kafka_broker_host = config["kafka_broker_host"]
        self.kafka_topic = config["kafka_starting_offsets"]
        self.check_point_location = config["check_point_location"]
        self.target_raw_data_path = config["target_raw_data_path"]

    def consume_kafka_topic(self, spark):
        # Defining sample schema for XML
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("content", StringType()),
            StructField("timestamp", DateType()),
            StructField("author", StructType([
                StructField("name", StringType()),
                StructField("email", StringType())
            ]))
        ])
        try:
            streaming_df = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_broker_host) \
                .option("subscribe", kafka_topic) \
                .option("startingOffsets", "specificOffsets") \
                .option("kafka.specificOffsets", json.dumps(self.kafka_starting_offsets)) \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("data"))
            return streaming_df
        except CustomSchemaException as e:
            raise CustomSchemaException(f"produce_message: exception in  schema - {str(e)}")
        except Exception as e:
                logging.error("Error in data consumption: %s", str(e))
                return {"error: ": str(e)}

    def deduplicate_data(self, df):
        """
        Implements de-duplication based on 'id' and 'timestamp' columns.
        """
        window_spec = Window.partitionBy("id").orderBy("timestamp")
        deduplicated_df = df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .where(col("row_number") == 1) \
            .drop("row_number")
        return deduplicated_df

    def dataframe_write(self,df) -> None:
        """
        Writes de-duplicated data to the raw layer in Parquet format.
        """
        raw_query = df \
            .writeStream \
            .format("parquet") \
            .option('checkpointLocation', self.checkpoint_loc) \
            .outputMode("append") \
            .partitionBy("date") \
            .trigger(self.trigger_interval)

        raw_query.start(self.target_raw_data_path)

        raw_query.awaitTermination()

    def process_raw_data(self) -> None:
            spark = initialize_spark_session()
            streaming_df = self.consume_kafka_topic(spark)
            deduplicate_df = self.deduplicate_data(streaming_df)
            self.dataframe_write(deduplicate_df)



def main():
    """
    Main function to execute the streaming pipeline.
    """
    sparkConsumer = SparkConsumer()
    sparkConsumer.process_raw_data()

# Execute the main function
if __name__ == "__main__":
    main()
