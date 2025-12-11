from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def filter_latest_df_version(df: DataFrame) -> DataFrame:
    """
    Filter the latest version of a DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with the latest version
    """
    print("Filtering latest version")
    result = df.select(F.max("version_date")).first()
    if result is None:
        return df
    latest = result[0]
    df = df.filter(F.col("version_date") == latest)
    return df


def normalize_ratings_df(df: DataFrame, user_stats_df: DataFrame) -> DataFrame:
    """
    Normalize ratings by subtracting the mean rating.

    Args:
        df: Input DataFrame
        user_stats_df: DataFrame with user statistics

    Returns:
        DataFrame with normalized ratings
    """
    print("Normalizing ratings")
    df = df.join(user_stats_df, on="UserID", how="left")
    df = df.withColumn(
        "Rating",
        F.when(
            F.col("std_rating") != 0,
            (F.col("Rating") - F.col("mean_rating")) / F.col("std_rating")
        ).otherwise(0)
    )
    return df
