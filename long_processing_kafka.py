import logging
import time
import argparse

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

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


def sleep_wrapper(column, sleep_time_secs: int):
    @pandas_udf(StringType())
    def sleep_udf(s: pd.Series) -> pd.Series:
        print(f"Pandas UDF series size is {len(s)}.   Will sleep for {sleep_time_secs} seconds...")
        time.sleep(sleep_time_secs)
        return s.str.upper()

    return sleep_udf(column)


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
    parser.add_argument("--batch_size", required=True)
    parser.add_argument("--sleep", required=True)
    args = parser.parse_args()

    spark = create_spark_session()
    hdfs_delete(spark, args.checkpoint_path)

    try:
        logger.info(f"Reading {args.batch_size} messages from EventHub {args.eh_namespace}/{args.eh_name} using Kafka connector")
        EVENT_HUB_SAS = f"Endpoint=sb://{args.eh_namespace}/;SharedAccessKeyName={args.eh_sas_name};SharedAccessKey={args.eh_sas_key}"
        EH_SASL = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUB_SAS}";'
        BOOTSTRAP_SERVERS = f"{args.eh_namespace}:9093"

        df = (
            spark.readStream.format("kafka")
            .option("subscribe", args.eh_name)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", args.batch_size)
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", EH_SASL)
            .option("kafka.request.timeout.ms", 60000)
            # .option("kafka.session.timeout.ms", 60000)
            # The following are recommended by Microsoft, see https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations
            # Azure closes inbound TCP idle > 240,000 ms, which can result in sending on dead connections (Must be < 240000)
            .option("kafka.connections.max.idle.ms", 180000)
            # Used for rebalance timeout, so this should not be set too low. Must be greater than session.timeout.ms.
            # It also has to be greater than the time required to process a micro-batch.  Let's make it 10 mins, but it still could break.
            .option("kafka.max.poll.interval.ms", 600000)
            # The max.poll.records setting seems to be ignored by Spark and replaced by 1
            # .option("kafka.max.poll.records", microBatchSize)
            # Can be lowered to pick up metadata changes sooner, but Microsoft recommends 180,000. (Must be < 240000)
            .option("kafka.metadata.max.age.ms", 180000)
            .load()
        )

        def batch_processing_udf(batch_df: DataFrame, batch_id: int):
            start_batch = time.time()
            logger.info(f"--- Processing Batch #{batch_id} with a Python UDF wrapping sleep({args.sleep}) ---")
            batch_df = batch_df.withColumn("sleep", sleep_wrapper(batch_df.topic, int(args.sleep)))
            print(f"Batch dataframe constains {batch_df.count()} rows.")
            logger.info(f"--- End of Batch #{batch_id} [elapsed: {round(time.time() - start_batch)} secs] ---")

        def batch_processing_sleep(batch_df: DataFrame, batch_id: int):
            start_batch = time.time()
            logger.info(f"--- Processing Batch #{batch_id} with Python time.sleep({args.sleep}) ---")
            time.sleep(int(args.sleep))
            print(f"Batch dataframe constains {batch_df.count()} rows.")
            logger.info(f"--- End of Batch #{batch_id} [elapsed: {round(time.time() - start_batch)} secs] ---")

        out = df.writeStream.option("checkpointLocation", args.checkpoint_path).foreachBatch(batch_processing_sleep).start()
        # out = df.writeStream.trigger(processingTime="6 minutes").foreachBatch(batch_processing_udf).start()
        # out = df.writeStream.trigger(processingTime="6 minutes").format("console").start()
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

# To enable verbose logs in Spark, use the following log4j2.properties file:
# /usr/local/spark/conf/log4j2.properties:
"""
status = warn

# Set everything to be logged to the console AND a file
rootLogger.level = warn
rootLogger.appenderRefs = stdout,logfile
rootLogger.appenderRef.stdout.ref = console
rootLogger.appenderRef.logfile.ref = file

# File logging properties
appender.file.type = File
appender.file.name = file
# Set this to where you want the log file to go
appender.file.fileName = /usr/local/spark/logs/spark.log
appender.file.layout.type = PatternLayout
appender.file.layout.pattern=%p\t%d{ISO8601}\t%r\t%c\t[%t]\t%m%n

# Console logging properties
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss} %p %c - %m [%t]%n%ex

# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
logger.repl.name = org.apache.spark.repl.Main
logger.repl.level = warn

logger.thriftserver.name = org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
logger.thriftserver.level = warn

# Settings to quiet third party logs that are too verbose
logger.spark.name = org.apache.spark
logger.spark.level = warn
logger.jetty1.name = org.sparkproject.jetty
logger.jetty1.level = warn
logger.jetty2.name = org.sparkproject.jetty.util.component.AbstractLifeCycle
logger.jetty2.level = error
logger.replexprTyper.name = org.apache.spark.repl.SparkIMain$exprTyper
logger.replexprTyper.level = info
logger.replSparkILoopInterpreter.name = org.apache.spark.repl.SparkILoop$SparkILoopInterpreter
logger.replSparkILoopInterpreter.level = info
logger.parquet1.name = org.apache.parquet
logger.parquet1.level = error
logger.parquet2.name = parquet
logger.parquet2.level = error


# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
logger.RetryingHMSHandler.name = org.apache.hadoop.hive.metastore.RetryingHMSHandler
logger.RetryingHMSHandler.level = fatal
logger.FunctionRegistry.name = org.apache.hadoop.hive.ql.exec.FunctionRegistry
logger.FunctionRegistry.level = error
logger.kafka010.name = org.apache.spark.sql.kafka010
logger.kafka010.level = trace
logger.kafkaclients.name = org.apache.kafka.clients
logger.kafkaclients.level = trace


# For deploying Spark ThriftServer
# SPARK-34128: Suppress undesirable TTransportException warnings involved in THRIFT-4805
appender.console.filter.1.type = RegexFilter
appender.console.filter.1.regex = .*Thrift error occurred during processing of message.*
appender.console.filter.1.onMatch = deny
appender.console.filter.1.onMismatch = neutral

# KAFKA-7509: Disable extraneous config warnings
# https://issues.apache.org/jira/browse/KAFKA-7509?focusedCommentId=16673371&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-16673371
logger.ConsumerConfig.name = org.apache.kafka.clients.consumer.ConsumerConfig
logger.ConsumerConfig.level = trace
"""
