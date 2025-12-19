from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import time

load_dotenv()


def monitor_progress(sc, interval=5):
    tracker = sc.statusTracker()
    while True:
        active_stages = tracker.getActiveStageIds()
        if not active_stages:
            break
        for stage_id in active_stages:
            stage_info = tracker.getStageInfo(stage_id)
            if stage_info:
                print(f"[Progress] Stage {stage_id}: {stage_info.numActiveTasks()} active / {stage_info.numTasks()} total")
        time.sleep(interval)


def get_spark_session(appName: str = "PythonClient") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(appName)   # type: ignore
        .master(os.getenv("SPARK_MASTER_ENDPOINT", "spark://spark-master:7077"))
        # .master("spark://172.21.0.2:7077")

        # --- EXECUTOR CONFIG (CRITICAL) ---
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "3")
        .config("spark.executor.instances", "2")
        # .config("spark.executor.memoryOverhead", "2g")

        # --- DRIVER SAFETY ---
        .config("spark.driver.memory", "3g")
        .config("spark.driver.maxResultSize", "512m")

        # --- STABILITY ---
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "30s")

        # More robust shuffle fetch retries
        .config("spark.shuffle.io.maxRetries", "10")
        .config("spark.shuffle.io.retryWait", "60s")

        # Prevent dynamic allocation from killing executors too early
        .config("spark.dynamicAllocation.enabled", "false")

        # --- SHUFFLE ---
        .config("spark.sql.shuffle.partitions", "1000")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", 200)
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # .config("spark.sql.files.maxPartitionBytes", "256MB")

        # --- SERIALIZATION ---
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.shuffle.spill.compress.codec", "lz4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")

        # --- S3 / MinIO ---
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config("spark.local.dir", "/tmp/spark-temp")

        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)

        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .getOrCreate()
    )

    # spark.sparkContext.setLogLevel("INFO")
    return spark


if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)

    df = spark.range(1, 5)
    df.show()

    spark.stop()
