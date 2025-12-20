from datetime import datetime
import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


# Get global variables
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")


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
    return stats


def compute_user_global_stats(user_data_df: DataFrame, save_path: str):
    print("Start computing statistics")
    stats = compute_user_global_stats_df(user_data_df)

    # Write data to S3
    stats = stats.withColumn("version_date", F.lit(CURRENT_DATE))
    stats.write.partitionBy("version_date").mode("overwrite").parquet(save_path)
    print(f"Done! Statistics written in {save_path}")
