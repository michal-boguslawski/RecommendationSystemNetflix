from dotenv import load_dotenv
import os
from pathlib import Path
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from ..utils.minio_utils import list_objects_in_bucket
from ..utils.spark_utils import get_spark_session
from .schemas import user_schema, movie_schema


env_path = os.path.join(Path(__file__).parent.parent.absolute(), ".env")
load_dotenv(env_path)


def preprocess_neflix_user_data_multiple_files(
    bucket_name: str = "recommendation-system",
    file_name: str = "combined_data",
    schema: StructType = user_schema,
    output_path: str = "s3a://recommendation-system/data/silver/netflix_user_data/v1",
):
    """
    Process Netflix user rating data from CSV files to Parquet format.

    Args:
        bucket_name: S3 bucket name
        file_name: File name without extension
        schema: Schema for the user data
        output_path: S3 path for output Parquet file
    """
    print("Preprocessing Netflix user data")
    spark = get_spark_session()
    files = list_objects_in_bucket(bucket_name=bucket_name, file_name=file_name)
    for file_path in files:
        print(f"Processing file {file_path}")
            # Read all matching files
        user_df = ( 
            spark.read
            .schema(schema)
            .option("sep", ",")
            .option("header", "false")
            .csv(file_path)
        )

        user_df = preprocess_netflix_user_data_file(
            user_df=user_df
        )    # Write to parquet

        user_df.write.mode("append").parquet(output_path)
        
        # clean
        spark.catalog.clearCache()
        print(f"Done! User data written in {output_path}")
        
    spark.stop()


def preprocess_netflix_user_data_file(
    user_df: DataFrame

) -> DataFrame:
    """
    Process Netflix user rating data from CSV files to Parquet format.
    
    Args:
        user_df: Input DataFrame
    
    Returns:
        Preprocessed DataFrame
    """
    print("Preprocessing Netflix user data")    
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

    # Cast UserID and MovieID to integer
    df_processed = df_processed.withColumn("UserID", F.col("UserID").cast("integer"))
    df_processed = df_processed.withColumn("MovieID", F.col("MovieID").cast("integer"))

    return df_processed


def preprocess_netflix_movie_data(
    input_path: str = "s3a://recommendation-system/data/bronze/movie_titles.csv",
    schema: StructType = movie_schema,
    output_path: str = "s3a://recommendation-system/data/silver/netflix_movie_data/v1"
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
