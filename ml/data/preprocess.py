from dotenv import load_dotenv
import os
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import StructType

from ml.utils.minio_utils import list_objects_in_bucket
from ml.utils.spark_utils import get_spark_session
from ml.data.schemas import schema_out, user_schema, movie_schema


load_dotenv()


def assign_movie_ids(rows):
    last_movie_id = None
    
    for r in rows:
        
        if r.UserID.endswith(":"):
            # Extract MovieID
            last_movie_id = r.UserID.replace(":", "").strip()
            continue
        
        yield Row(
            UserID=int(r.UserID) if r.UserID else None,
            Rating=r.Rating,
            Date=r.Date,
            MovieID=int(last_movie_id) if last_movie_id else None
        )

def preprocess_neflix_user_data_multiple_files(
    bucket_name: str = "recommendation-system",
    file_name: str = "combined_data",
    schema: StructType = user_schema,
    output_path: str = "s3a://recommendation-system/data/silver/netflix_user_data.parquet",
    coalesce_files: int = 4
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
    files = list_objects_in_bucket(bucket_name=bucket_name, file_name=file_name)
    for file_path in files:
        print(f"Processing file {file_path}")
        preprocess_netflix_user_data_file(input_path=file_path, schema=schema, output_path=output_path, coalesce_files=coalesce_files)
    


def preprocess_netflix_user_data_file(
    input_path: str = "s3a://recommendation-system/data/bronze/combined_data_*.txt",
    schema: StructType = user_schema,
    output_path: str = "s3a://recommendation-system/data/silver/netflix_user_data.parquet",
    coalesce_files: int = 4

) -> None:
    """
    Process Netflix user rating data from CSV files to Parquet format.
    
    Args:
        input_path: S3 path to input CSV files
        schema: Schema for the user data
        output_path: S3 path for output Parquet file
    """
    print("Preprocessing Netflix user data")
    spark = get_spark_session()

    # Read all matching files
    df = spark.read.schema(schema).option("sep", ",").csv(input_path)
    df = df.withColumn("file", F.input_file_name())
    df = df.repartition(1, "file")

    # df is your DataFrame with is_movie_header column
    rdd = df.rdd.mapPartitions(assign_movie_ids)
    df_out = spark.createDataFrame(rdd, schema=schema_out)

    # Write to parquet
    df_out.coalesce(coalesce_files).write.mode("append").parquet(output_path)
    
    # clean
    df.unpersist() if "df" in locals() else None
    spark.catalog.clearCache()
    spark.stop()
    print(f"Done! User data written in {output_path}")


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
