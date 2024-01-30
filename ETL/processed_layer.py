import logging
from valdation import data_valdiation
from util import initialize_spark_session



class ProcessedLayer:
    def __init__(
        self,
    ) -> None:
        logging.basicConfig(level=logging.INFO)
        with open("streaming_cofig.yaml", "r") as yamlfile:
            config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        self.config = config
        self.target_raw_data_path = config["target_raw_data_path"]
        self.target_processed_data_path = config["target_processed_data_path"]
        self.target_error_data_path = config["target_error_data_path"]



    def read_data(self,spark):
        """
        Processes the data by parsing XML and flattening the structure.
        """
        processed_df = spark \
            .read \
            .parquet(self.target_raw_data_path) \
            .withColumn("timestamp", col("data.timestamp").cast(TimestampType())) \
            .withColumn("date", to_date("timestamp")) \
            .withColumn("author_name", expr("trim(data.author.name)")) \
            .withColumn("author_email", expr("lower(trim(data.author.email))")) \
            .drop("data")
        return processed_df


    def select_delta_records(self,df):
        """
        Selects delta records based on timestamp for further processing.
        """
        delta_df = df \
            .where(col("timestamp") > expr("current_timestamp() - interval "))
        return delta_df

    def write_to_processed_layer(self, df):
        """
        Writes delta records to the processed layer in Parquet format.
        """
        valid_df = df.filter( < condition_expression >)
        error_df = df.except(valid_df)

        if valid_df.count() > 0:
             valid_df.write.parquet(self.target_processed_data_path)
        if error_df.count() > 0:
            error_df.write.parquet(self.target_error_data_path)

    def process_data(self):
        spark = initialize_spark_session()
        processed_df = read_data(spark)
        validated_df = data_valdiation(processed_df)
        delta_df = select_delta_records(validated_df)
        write_to_processed_layer(delta_df)


def main():
    """
    Main function to execute the streaming pipeline.
    """
    processedLayer = ProcessedLayer()
    processedLayer.process_data()

# Execute the main function
if __name__ == "__main__":
    main()

