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
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data.parquet"
USER_STATS_PATH = f"s3a://{BUCKET}/data/gold/user_stats/"
USER_USER_CORR_PATH = f"s3a://{BUCKET}/data/gold/user_user_corr/"
ITEM_ITEM_CORR_PATH = f"s3a://{BUCKET}/data/gold/item_item_corr/"


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


def compute_user_global_stats():
    print("Start computing statistics")

    # Start spark session
    spark = get_spark_session()

    # Read data and compute statistics
    df = spark.read.parquet(USER_DATA_PATH)
    stats = compute_user_global_stats_df(df)

    # Write data to S3
    stats.write.partitionBy("version_date").mode("overwrite").parquet(USER_STATS_PATH)
    print(f"Done! Statistics written in {USER_STATS_PATH}")

    # Clean
    spark.stop()


def compute_item_item_correlation_df(df: DataFrame) -> DataFrame:
    """
    Compute item-item correlation.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with item-item correlation
    """
    print("Computing item-item correlation")
    # Join data to get item-item pairs ratings
    pairs = (
        df.alias("a")
        .join(df.alias("b"),
                (F.col("a.UserID") == F.col("b.UserID")) &
                (F.col("a.MovieID") < F.col("b.MovieID")))
        .select(
            F.col("a.MovieID").alias("movie1"),
            F.col("b.MovieID").alias("movie2"),
            F.col("a.Rating").alias("rating1"),
            F.col("b.Rating").alias("rating2")
        )
    )

    # Calculate correlations between items
    item_corr = (
        pairs.groupBy("movie1", "movie2")
            .agg(
                F.corr("rating1", "rating2").alias("correlation"),
                F.count("movie1").alias("corated_users")
            )
    )

    # Filter out correlations with less than 2 co-rated movies
    item_corr = item_corr.filter(F.col("corated_users") >= 2)

    # Add date
    item_corr = item_corr.withColumn("version_date", F.lit(CURRENT_DATE))
    
    return item_corr


def compute_item_item_correlation() -> None:
    """
    Compute item-item correlation.

    Returns:
        None
    """
    print("Start computing item-item correlation")

    # Start spark session
    spark = get_spark_session()

    # Read data and compute statistics
    user_data_df = spark.read.parquet(USER_DATA_PATH)
    user_data_df = user_data_df.repartition("UserID")
    user_data_df.persist(StorageLevel.DISK_ONLY)

    user_stats_df = spark.read.parquet(USER_STATS_PATH)
    user_stats_df = filter_latest_df_version(user_stats_df)
    
    # Normalize user rating
    normalized_user_data_df = normalize_ratings_df(user_data_df, user_stats_df)
    normalized_user_data_df = normalized_user_data_df.repartition("UserID")
    normalized_user_data_df.persist(StorageLevel.DISK_ONLY)

    # Compute user - user correlations
    corr = compute_item_item_correlation_df(normalized_user_data_df)

    # Write data to S3
    corr.write.partitionBy("version_date").mode("overwrite").parquet(ITEM_ITEM_CORR_PATH)

    # Clean memory
    user_data_df.unpersist()
    normalized_user_data_df.unpersist()
    print(f"Done! Correlation written in {ITEM_ITEM_CORR_PATH}")

    # Clean
    spark.stop()


def compute_user_user_correlation_df(df: DataFrame) -> DataFrame:
    """
    Compute user-user correlation.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with user-user correlation
    """
    print("Computing user-user correlation")
    # Join data to get user-user pairs ratings
    pairs = (
        df.alias("a")
        .join(df.alias("b"),
                (F.col("a.MovieID") == F.col("b.MovieID")) &
                (F.col("a.UserID") < F.col("b.UserID")))
        .select(
            F.col("a.UserID").alias("user1"),
            F.col("b.UserID").alias("user2"),
            F.col("a.Rating").alias("rating1"),
            F.col("b.Rating").alias("rating2")
        )
    )

    # Calculate correlations between users
    user_corr = (
        pairs.groupBy("user1", "user2")
            .agg(
                F.corr("rating1", "rating2").alias("correlation"),
                F.count("user1").alias("corated_movies")
            )
    )

    # Filter out correlations with less than 2 co-rated movies
    user_corr = user_corr.filter(F.col("corated_movies") >= 2)

    # Add date
    user_corr = user_corr.withColumn("version_date", F.lit(CURRENT_DATE))
    
    return user_corr


def compute_user_user_correlation() -> None:
    """
    Compute user-user correlation.

    Returns:
        None
    """
    print("Start computing user-user correlation")

    # Start spark session
    spark = get_spark_session()

    # Read data and compute statistics
    user_data_df = spark.read.parquet(USER_DATA_PATH)
    user_data_df = user_data_df.repartition("MovieID")
    user_data_df.persist(StorageLevel.DISK_ONLY)

    user_stats_df = spark.read.parquet(USER_STATS_PATH)
    user_stats_df = filter_latest_df_version(user_stats_df)
    
    # Normalize user rating
    normalized_user_data_df = normalize_ratings_df(user_data_df, user_stats_df)
    normalized_user_data_df = normalized_user_data_df.repartition("MovieID")
    normalized_user_data_df.persist(StorageLevel.DISK_ONLY)

    # Compute user - user correlations
    corr = compute_user_user_correlation_df(normalized_user_data_df)

    # Write data to S3
    corr.write.partitionBy("version_date").mode("overwrite").parquet(USER_USER_CORR_PATH)

    # Clean memory
    user_data_df.unpersist()
    normalized_user_data_df.unpersist()
    print(f"Done! Correlation written in {USER_USER_CORR_PATH}")

    # Clean
    spark.stop()


if __name__ == "__main__":
    compute_user_global_stats()
    # compute_user_user_correlation()  # not possible due to large dataset
    # compute_item_item_correlation()  # not possible due to large dataset
