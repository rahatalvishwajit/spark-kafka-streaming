from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Kafka Streaming POC") \
        .master("local") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "spark.kafka.streaming") \
        .option("startingOffsets", "latest") \
        .load()

    df.printSchema()

    customSchema = StructType() \
        .add("RecordNumber", IntegerType(), True) \
        .add("Zipcode", IntegerType(), True) \
        .add("ZipCodeType", StringType(), True) \
        .add("City", StringType(), True) \
        .add("State", StringType(), True) \
        .add("LocationType", StringType(), True) \
        .add("AddressLineOne", FloatType(), True) \
        .add("AddressLineTwo", FloatType(), True) \
        .add("Country", StringType(), True) \
        .add("Purpose", StringType(), True)

    df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")

    df2 = df1.select(from_json(col("value"), customSchema).alias("parsed_value"), "timestamp")

    df3 = df2.select("parsed_value.*", "timestamp")

    df3.printSchema()
    '''
    df4 = df3.groupby("RecordNumber") \
        .agg(*[first(x, ignorenulls = True) for x in df3.columns if x!='RecordNumber'])
    '''

    writeOutputDF = df1.writeStream \
        .format("console") \
        .queryName("Spark Kafka Streaming - Address Details") \
        .outputMode("update") \
        .option("truncate", "false") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 second") \
        .start()

    writeOutputDF.awaitTermination()
