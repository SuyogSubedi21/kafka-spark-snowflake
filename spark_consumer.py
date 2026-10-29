import os

import snowflake.connector
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

TOPIC = "api_orders"
CHECKPOINT = "./checkpoints/orders"

spark = (
    SparkSession.builder
    .appName("kafka_spark_snowflake")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator="SNOWFLAKE_JWT",
    private_key_file=os.path.expanduser(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="RAW",
)

fields = [
    "order_id", "order_date", "company", "product_id", "product_name", "category",
    "quantity", "cost_price", "sell_price", "discount_pct", "revenue",
    "region", "payment_method", "customer_id", "is_returned",
]
schema = StructType([StructField(f, StringType()) for f in fields])


def to_number(column):
    return F.regexp_replace(F.col(column), "[^0-9.-]", "").cast("double")


raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
)

parsed = raw.withColumn("o", F.from_json("raw_value", schema)).select(
    "raw_value", "kafka_partition", "kafka_offset", "o.*"
)

cleaned = parsed.select(
    "raw_value", "kafka_partition", "kafka_offset",
    to_number("order_id").cast("int").alias("order_id"),
    F.coalesce(
        F.to_date("order_date", "yyyy-MM-dd"),
        F.to_date("order_date", "dd/MM/yyyy"),
        F.to_date("order_date", "MMM dd yyyy"),
    ).alias("order_date"),
    F.initcap(F.trim("company")).alias("company"),
    F.upper(F.trim("product_id")).alias("product_id"),
    F.trim("product_name").alias("product_name"),
    F.initcap(F.trim("category")).alias("category"),
    to_number("quantity").cast("int").alias("quantity"),
    to_number("cost_price").alias("cost_price"),
    to_number("sell_price").alias("sell_price"),
    to_number("discount_pct").alias("discount_pct"),
    to_number("revenue").alias("revenue"),
    F.initcap(F.trim("region")).alias("region"),
    F.initcap(F.trim("payment_method")).alias("payment_method"),
    F.upper(F.trim("customer_id")).alias("customer_id"),
    F.when(F.lower(F.trim("is_returned")).isin("yes", "y", "true", "1"), True)
     .when(F.lower(F.trim("is_returned")).isin("no", "n", "false", "0"), False)
     .alias("is_returned"),
)

validated = cleaned.withColumn(
    "reason",
    F.when(F.col("order_id").isNull(), "missing order_id")
     .when(F.col("order_date").isNull(), "bad order_date")
     .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "bad quantity")
     .when(F.col("sell_price").isNull(), "bad sell_price"),
)


def write_batch(batch_df, batch_id):
    batch_df = batch_df.withColumn("loaded_at", F.current_timestamp())

    good = batch_df.filter(F.col("reason").isNull()).drop("reason", "raw_value")
    bad = batch_df.filter(F.col("reason").isNotNull()).select(
        "raw_value", "reason", "kafka_partition", "kafka_offset", "loaded_at"
    )

    good_pd = good.toPandas()
    bad_pd = bad.toPandas()

    if not good_pd.empty:
        good_pd.columns = [c.upper() for c in good_pd.columns]
        write_pandas(conn, good_pd, "SPARK_ORDERS_CLEAN", use_logical_type=True)
    if not bad_pd.empty:
        bad_pd.columns = [c.upper() for c in bad_pd.columns]
        write_pandas(conn, bad_pd, "SPARK_ORDERS_REJECTED", use_logical_type=True)

    print(f"Batch {batch_id}: {len(good_pd)} clean, {len(bad_pd)} rejected")


query = (
    validated.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Spark consumer started. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping...")
    query.stop()
finally:
    conn.close()
    spark.stop()