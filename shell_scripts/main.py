spark-submit --executor-memory 12g --driver-memory 12g --executor-cores 5 --num-executors 32 \
        --conf spark.executor.memoryOverhead=2g
        --conf spark.default.parallelism=200
        --conf spark.sql.shuffle.partitions=600 \
        --py-files kafka_publish.py


spark-submit --executor-memory 12g --driver-memory 12g --executor-cores 5 --num-executors 32 \
        --conf spark.executor.memoryOverhead=2g
        --conf spark.default.parallelism=200
        --conf spark.sql.shuffle.partitions=600 \
        --py-files raw_layer.py



#setup a cron expression to trigger hourly batch job - 0 * * * *
spark-submit --executor-memory 12g --driver-memory 12g --executor-cores 5 --num-executors 32 \
        --conf spark.executor.memoryOverhead=2g
        --conf spark.default.parallelism=200
        --conf spark.sql.shuffle.partitions=600 \
        --py-files processed_layer.py

#setup an archival process with specific time to move data from raw layer to archival layer periodically (TLL is also present )
