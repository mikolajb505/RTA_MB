from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,to_timestamp 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
spark = (
    SparkSession.builder
    .appName("Lab4-Kafka")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
 
SCHEMA = tx_schema = StructType([
    StructField("tx_id",     StringType()),
    StructField("user_id",   StringType()),
    StructField("amount",    DoubleType()),
    StructField("store",     StringType()),
    StructField("category",  StringType()),
    StructField("timestamp", StringType()),
])
 
kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .load()
)
parsed = (kafka_raw.select(
    from_json(col("value").cast("string"), SCHEMA).alias("tx"))
    .select("tx.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
         )
q = (
    parsed.writeStream
    .format("console") 
    .outputMode("append")
    .option("truncate", False)
    .start()
)
q.awaitTermination()
