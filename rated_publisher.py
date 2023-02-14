import logging
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local")
        .config("spark.sql.optimizer.enableJsonExpressionOptimization", False)
        .config("spark.ui.showConsoleProgress", False)
        .config("spark.app.name", "Kafka long lived processing test")
        .config("spark.cores.max", 1)
        .config("spark.executor.cores", 1)
        .config("spark.executor.memory", "2g")
        .config("spark.dynamicAllocation.enabled", True)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
        .config("spark.sql.adaptive.enabled", False)
        .config("spark.network.timeout", "600s")
        .config("spark.dynamicAllocation.executorIdleTimeout", 600)
        .config("spark.sql.shuffle.partitions", 10)
        .config("spark.network.crypto.enabled", False)
        # .config(
        #     "spark.driver.extraJavaOptions", "-javaagent:/home/jovyan/jSSLKeyLog.jar=/home/jovyan/kafka_tls_keys.log"
        # )
        .getOrCreate()
    )


def hdfs_delete(spark: SparkSession, path_to_delete: str):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(path_to_delete)
    if fs.exists(path):
        fs.delete(path, True)


def main():
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-5s] " + "%(message)s (%(module)s)",
        level=logging.INFO,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--eh_namespace", required=True)
    parser.add_argument("--eh_name", required=True)
    parser.add_argument("--eh_sas_name", required=True)
    parser.add_argument("--eh_sas_key", required=True)
    parser.add_argument("--checkpoint_path", required=True)
    parser.add_argument("--rows_per_second", required=True)
    args = parser.parse_args()

    spark = create_spark_session()
    hdfs_delete(spark, args.checkpoint_path)

    try:
        logger.info(
            f"Writing messages to EventHub {args.eh_namespace}/{args.eh_name} using Kafka connector at a rate of {args.rows_per_second} messages per second."
        )
        EVENT_HUB_SAS = f"Endpoint=sb://{args.eh_namespace}/;SharedAccessKeyName={args.eh_sas_name};SharedAccessKey={args.eh_sas_key}"
        EH_SASL = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUB_SAS}";'
        BOOTSTRAP_SERVERS = f"{args.eh_namespace}:9093"

        df = spark.readStream.format("rate").option("rowsPerSecond", args.rows_per_second).load()
        df = df.withColumn("value", col("value").cast("string"))
        out = (
            df.writeStream.queryName("EventHub output")
            .format("kafka")
            .outputMode("append")
            .option("topic", args.eh_name)
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", EH_SASL)
            .option("checkpointLocation", args.checkpoint_path)
            .start()
        )
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Job interrupted, stopping queries and exiting.")
        if out:
            out.stop()
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
