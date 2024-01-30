def validate_data(processed_df):
    """
    Validates the data by filtering out null values and ensuring data integrity.
    """
    validated_df = processed_df \
        .filter(col("data.id").isNotNull()) \
        .filter(col("data.content").isNotNull()) \
        .filter(col("data.timestamp").isNotNull()) \
        .filter(col("data.author.name").isNotNull()) \
        .filter(col("data.author.email").isNotNull()) \
        .withColumn("timestamp", col("data.timestamp").cast(TimestampType()))
    return validated_df