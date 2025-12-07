from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def compute_global_stats(df: DataFrame) -> DataFrame:
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
        F.stddev_pop("Rating").alias("std_rating")
    )
    return stats


if __name__ == "__main__":
    from ml.utils.spark_utils import get_spark_session
    spark = get_spark_session()
    df = spark.read.parquet("s3a://recommendation-system/data/silver/netflix_user_data.parquet")
    # df = df.limit(1000)
    stats = compute_global_stats(df)
    stats.show()
    spark.stop()
