def initialize_spark_session():
    """
    Initializes SparkSession for Spark operations.
    """
    spark = SparkSession.builder \
        .appName("Streaming XML to Parquet") \
        .getOrCreate()
    return spark
