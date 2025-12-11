from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession


load_dotenv()


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PythonClient")   # type: ignore

        .master(os.getenv("SPARK_MASTER_ENDPOINT", "local[*]"))
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.hadoop.metrics2.console.period", "0")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        # .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        # .config("spark.default.parallelism", os.getenv("SPARK_DEFAULT_PARALLELISM", "4"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
