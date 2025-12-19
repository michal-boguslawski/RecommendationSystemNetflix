from datetime import datetime
import os
from pyspark import StorageLevel
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from .utils import filter_latest_df_version, normalize_ratings_df
from ml.utils.spark_utils import get_spark_session


# Get global variables
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data/v1"
USER_STATS_PATH = f"s3a://{BUCKET}/data/gold/user_stats/v1"


def compute_user_global_stats_df(df: DataFrame) -> DataFrame:
    """
    Compute global statistics for a DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with global statistics
    """
    print("Computing global statistics")
    stats = df.groupBy("UserID").agg(
        F.mean("Rating").alias("mean_rating"),
        F.stddev_pop("Rating").alias("std_rating"),
        F.max("Rating").alias("max_rating"),
        F.min("Rating").alias("min_rating"),
        F.sum(F.lit(1)).alias("rated_movies"),
    )
    stats = stats.withColumn("version_date", F.lit(CURRENT_DATE))
    return stats


def compute_user_global_stats(user_data_df: DataFrame):
    print("Start computing statistics")
    stats = compute_user_global_stats_df(user_data_df)

    # Write data to S3
    stats.write.partitionBy("version_date").mode("overwrite").parquet(USER_STATS_PATH)
    print(f"Done! Statistics written in {USER_STATS_PATH}")


if __name__ == "__main__":
    # Start spark session
    spark = get_spark_session()

    # Read data and compute statistics
    user_data_df = spark.read.parquet(USER_DATA_PATH)
    compute_user_global_stats(user_data_df)

    # Clean
    spark.stop()
    # compute_user_user_correlation()  # not possible due to large dataset
    # compute_item_item_correlation()  # not possible due to large dataset
