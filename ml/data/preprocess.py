from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from ml.utils.minio_utils import list_objects_in_bucket
from ml.utils.spark_utils import get_spark_session
from ml.data.schemas import user_schema, movie_schema


load_dotenv()


def preprocess_neflix_user_data_multiple_files(
    bucket_name: str = "recommendation-system",
    file_name: str = "combined_data",
    schema: StructType = user_schema,
    output_path: str = "s3a://recommendation-system/data/silver/netflix_user_data.parquet",
    invalid_output_path: str = "s3a://recommendation-system/data/silver/netflix_user_data_invalid.parquet",
):
    """
    Process Netflix user rating data from CSV files to Parquet format.

    Args:
        bucket_name: S3 bucket name
        file_name: File name without extension
        schema: Schema for the user data
        output_path: S3 path for output Parquet file
        invalid_output_patch: S3 path for output Parquet file containing invalid rows
    """
    print("Preprocessing Netflix user data")
    spark = get_spark_session()
    files = list_objects_in_bucket(bucket_name=bucket_name, file_name=file_name)
    for file_path in files:
        print(f"Processing file {file_path}")
        preprocess_netflix_user_data_file(
            spark=spark,
            input_path=file_path,
            schema=schema,
            output_path=output_path,
            invalid_output_path=invalid_output_path
        )
    spark.stop()


def preprocess_netflix_user_data_file(
    input_path: str,
    schema: StructType,
    output_path: str,
    invalid_output_path: str,
    spark: SparkSession | None = None,

) -> None:
    """
    Process Netflix user rating data from CSV files to Parquet format.
    
    Args:
        input_path: S3 path to input CSV files
        schema: Schema for the user data
        output_path: S3 path for output Parquet file
        invalid_output_patch: S3 path for output Parquet file containing invalid rows
    """
    print("Preprocessing Netflix user data")
    if_close_spark = False
    if spark is None:
        spark = get_spark_session()
        if_close_spark = True
        

    # Read all matching files
    user_df = ( 
        spark.read
        .schema(schema)
        .option("sep", ",")
        .option("header", "false")
        .csv(input_path)
    )
    
    user_df = user_df.withColumn(
        "is_movie",
        F.col("UserID").endswith(":")
    )

    user_df = user_df.withColumn(
        "MovieID",
        F.when(F.col("is_movie"), F.regexp_replace("UserID", ":", ""))
    )
    
    w = Window.orderBy(F.monotonically_increasing_id())

    user_df = user_df.withColumn(
        "MovieID",
        F.last("MovieID", ignorenulls=True).over(w)
    )
    
    user_df = user_df.filter(~F.col("is_movie"))
    
    df_processed = user_df.select("UserID", "Rating", "Date", "MovieID")

    # Write to parquet
    df_processed.write.mode("append").parquet(output_path)
    
    # clean
    spark.catalog.clearCache()
    print(f"Done! User data written in {output_path}")
    if if_close_spark:
        spark.stop()


def preprocess_netflix_movie_data(
    input_path: str = "s3a://recommendation-system/data/bronze/movie_titles.csv",
    schema: StructType = movie_schema,
    output_path: str = "s3a://recommendation-system/data/silver/netflix_movie_data.parquet"
) -> None:
    """
    Process Netflix movie metadata from CSV to Parquet format.
    
    Args:
        input_path: S3 path to input CSV file
        schema: Schema for the movie data
        output_path: S3 path for output Parquet file
    """
    print("Preprocessing Netflix movie data")
    spark = get_spark_session()

    # Read all matching files
    df = spark.read.schema(schema).option("sep", ",").csv(input_path)

    # Write to parquet
    df.write.mode("overwrite").parquet(output_path)
    spark.catalog.clearCache()
    spark.stop()
    print(f"Done! Movie data written in {output_path}")


if __name__ == "__main__":
    print(20 * "=", "Preprocessing Netflix data", 20 * "=")
    BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
    OUTPUT_PATH = "s3a://recommendation-system/data/silver/netflix_user_data.parquet"
    preprocess_neflix_user_data_multiple_files(
        bucket_name=BUCKET,
        file_name="combined_data",
        output_path=OUTPUT_PATH
    )
    # preprocess_netflix_user_data_file()
    preprocess_netflix_movie_data()
    print(20 * "=", "Done preprocessing data!", 20 * "=")
