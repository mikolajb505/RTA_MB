## uruchom przez spark-submit streamrate.py

# połączenie do sparka - trzeba zawsze wpisać
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream # read straam a nie samo read jak do df
      .format("rate") # źródło
      .option("rowsPerSecond", 1)
      .load()
)


query = (df.writeStream 
    .format("console") 
    .outputMode("append") 
    .option("truncate", False) 
    .start()
) 

query.awaitTermination()  # wyzwalacz zakończenia dajemy żeby sam się nie zakończył
