from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession


load_dotenv()


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PythonClient")   # type: ignore

        .master(os.getenv("SPARK_MASTER_ENDPOINT", "local[*]"))
        # --- FIX SHUFFLE FETCH FAILURES ---
        # External shuffle service (MUST also be running on workers)
        # .config("spark.shuffle.service.enabled", "true")
        # docker exec -it spark-worker-1 bash
        # $SPARK_HOME/sbin/start-shuffle-service.sh

        # Allow more time before marking executor as dead
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "30s")

        # More robust shuffle fetch retries
        .config("spark.shuffle.io.maxRetries", "10")
        .config("spark.shuffle.io.retryWait", "60s")

        # Prevent dynamic allocation from killing executors too early
        .config("spark.dynamicAllocation.enabled", "false")

        # --- MEMORY / STABILITY IMPROVEMENTS ---
        # .config("spark.executor.memory", "2g")
        # .config("spark.executor.cores", "1")        # safer on small nodes
        # .config("spark.driver.memory", "2g")

        # --- SHUFFLE & SERIALIZATION ---
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.shuffle.spill.compress.codec", "lz4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # --- S3 / MinIO ---
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .getOrCreate()
    )
    return spark


if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)

    df = spark.range(1, 5)
    df.show()

    spark.stop()
