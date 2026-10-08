from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


POSTGRES_URL = "jdbc:postgresql://postgres:5432/frauddb"

POSTGRES_PROPERTIES = {
    "user": "frauduser",
    "password": "fraudpass",
    "driver": "org.postgresql.Driver"
}

def write_processed(batch_df, batch_id):

    batch_df.write \
        .mode("append") \
        .jdbc(
            url=POSTGRES_URL,
            table="processed_transactions",
            properties=POSTGRES_PROPERTIES
        )

    print(f"Batch {batch_id} written.")


def write_alerts(batch_df, batch_id):

    batch_df.write \
        .mode("append") \
        .jdbc(
            url=POSTGRES_URL,
            table="fraud_alerts",
            properties=POSTGRES_PROPERTIES
        )

    print(f"Alert batch {batch_id} written.")

# ==================================================
# SPARK SESSION
# ==================================================

spark = SparkSession.builder \
    .appName("FraudDetectionEngine") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==================================================
# KAFKA SOURCE
# ==================================================

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# ==================================================
# SCHEMA
# ==================================================

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device", StringType(), True),
    StructField("user_country", StringType(), True),
    StructField("transaction_country", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# ==================================================
# PARSE JSON
# ==================================================

transactions = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# ==================================================
# TIMESTAMP
# ==================================================

transactions = transactions.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

transactions = transactions.withColumn(
    "hour",
    hour(col("event_time"))
)

# ==================================================
# FRAUD RULES
# ==================================================

transactions = transactions.withColumn(
    "amount_score",
    when(col("amount") > 3000, 30).otherwise(0)
)

transactions = transactions.withColumn(
    "device_score",
    when(col("device") == "new_device", 20).otherwise(0)
)

transactions = transactions.withColumn(
    "country_score",
    when(
        col("user_country") != col("transaction_country"),
        20
    ).otherwise(0)
)

transactions = transactions.withColumn(
    "night_score",
    when(
        (col("hour") >= 0) &
        (col("hour") <= 5),
        15
    ).otherwise(0)
)

transactions = transactions.withColumn(
    "merchant_score",
    when(
        col("merchant_category").isin(
            "electronics",
            "gaming",
            "travel"
        ),
        15
    ).otherwise(0)
)

# ==================================================
# TOTAL SCORE
# ==================================================

transactions = transactions.withColumn(
    "fraud_score",
    col("amount_score")
    + col("device_score")
    + col("country_score")
    + col("night_score")
    + col("merchant_score")
)

# ==================================================
# RISK LEVEL
# ==================================================

transactions = transactions.withColumn(
    "risk_level",
    when(col("fraud_score") >= 75, "CRITICAL")
    .when(col("fraud_score") >= 50, "HIGH")
    .when(col("fraud_score") >= 30, "MEDIUM")
    .otherwise("LOW")
)

# ==================================================
# FRAUD REASONS
# ==================================================

transactions = transactions.withColumn(
    "fraud_reasons",
    concat_ws(
        ", ",
        when(col("amount_score") > 0, lit("High Amount")),
        when(col("device_score") > 0, lit("New Device")),
        when(col("country_score") > 0, lit("Different Country")),
        when(col("night_score") > 0, lit("Night Transaction")),
        when(col("merchant_score") > 0, lit("Risky Merchant"))
    )
)

# ==================================================
# ALERT FLAG
# ==================================================

transactions = transactions.withColumn(
    "is_fraud_alert",
    when(
        col("risk_level").isin("HIGH", "CRITICAL"),
        True
    ).otherwise(False)
)

# ==================================================
# SELECT FINAL COLUMNS
# ==================================================
processed = transactions.select(
    "transaction_id",
    "user_id",
    "merchant_id",
    "amount",
    "currency",
    "merchant_category",
    "payment_method",
    "device",
    "user_country",
    "transaction_country",
    col("event_time").alias("transaction_timestamp"),
    "fraud_score",
    "risk_level",
    "fraud_reasons",
    "is_fraud_alert"
)

alerts = processed.filter(
    col("risk_level").isin("HIGH", "CRITICAL")
)



alerts_for_db = alerts.select(
    "transaction_id",
    "user_id",
    "amount",
    "fraud_score",
    "risk_level",
    "fraud_reasons",
    "transaction_timestamp"
)

alerts_kafka = alerts.select(
    to_json(
        struct(
            "transaction_id",
            "user_id",
            "amount",
            "fraud_score",
            "risk_level",
            "fraud_reasons",
            "transaction_timestamp"
        )
    ).alias("value")
)

processed_query = processed.writeStream \
    .foreachBatch(write_processed) \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/processed_transactions"
    ) \
    .outputMode("append") \
    .start()

alerts_query = alerts_for_db.writeStream \
    .foreachBatch(write_alerts) \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/postgres_alerts"
    ) \
    .outputMode("append") \
    .start()
alerts_kafka_query = alerts_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "/tmp/checkpoints/fraud_alerts") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()