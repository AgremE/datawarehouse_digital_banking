from pyspark.sql import SparkSession
from io_helper import ReadWriteManager
from pyspark.sql.functions import sum as sum_

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("TransformCurated")
        .master("local[*]")
        .getOrCreate()
    )
    rw = ReadWriteManager(spark, "conf/tables")
    tx = rw.read("transactions")
    daily = tx.groupBy("account_id", "transaction_date").agg(
        sum_("amount").alias("daily_amount")
    )
    rw.write(daily, "daily_summary")
    spark.stop()
