import argparse, json
from pyspark.sql import SparkSession, functions as F
from src.config import KAFKA_BOOTSTRAP_SERVERS
from src.utils.logger import get_logger

logger = get_logger("spark-producer")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--topic", required=True)
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("ecom-producer")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    df = (spark.read.option("header", True).csv(args.csv))
    # Casts básicos
    numeric_cols = ["qtde","preco_unitario","valor_total"]
    for c in numeric_cols:
        df = df.withColumn(c, F.col(c).cast("double"))
    df = df.withColumn("pedido_id", F.col("pedido_id").cast("string"))

    # Monta key/value para Kafka (value em JSON)
    value_cols = [c for c in df.columns if c != "pedido_id"]
    out = (df
           .select(F.col("pedido_id").alias("key"),
                   F.to_json(F.struct(*value_cols)).alias("value"))
           .select(F.col("key").cast("string"),
                   F.col("value").cast("string")))

    (out.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", args.topic)
        .save())

    logger.info("Envio para Kafka concluído.")
    spark.stop()

if __name__ == "__main__":
    main()
