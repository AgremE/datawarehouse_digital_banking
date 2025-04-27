from pyspark.sql import SparkSession
from io_helper import ReadWriteManager

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IngestRaw").master("local[*]").getOrCreate()
    rw = ReadWriteManager(spark, "conf/tables")
    df = rw.read("transactions")
    rw.write(df, "transactions")
    spark.stop()
