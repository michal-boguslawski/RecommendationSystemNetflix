import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .spark_utils import get_spark_session


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data.parquet"


def get_user_rating(user_id: int | list, user_data_df: DataFrame):
    
    if isinstance(user_id, list):
        for u_id in user_id:
            print(f"User {u_id} rating:")
            get_user_rating(user_id=u_id, user_data_df=user_data_df)
    
    else:
        user_data_df.filter(F.col("UserID") == user_id).show(truncate=False)


def get_both_users_rated_movies(user_id1: int, user_id2: int, user_data_df: DataFrame):
    user1_df = user_data_df.filter(F.col("UserID") == user_id1)
    user2_df = user_data_df.filter(F.col("UserID") == user_id2)
    
    pairs_user_ratings = (
        user1_df.alias("a")
        .join(user2_df.alias("b"),
                (F.col("a.MovieID") == F.col("b.MovieID")))
        .select(
            F.col("a.MovieID").alias("movie_id"),
            F.col("a.UserID").alias("user1"),
            F.col("b.UserID").alias("user2"),
            F.col("a.Rating").alias("rating1"),
            F.col("b.Rating").alias("rating2")
        )
    )
    
    pairs_user_ratings.show(truncate=False)


def filter_user(all_user_data_df: DataFrame, user_id: int) -> DataFrame:
    user_df = (
        all_user_data_df
        .filter(F.col("UserID") == user_id)
    )
    return user_df


if __name__ == "__main__":
    # Start spark session
    spark = get_spark_session()

    # Read data
    user_data_df = spark.read.parquet(USER_DATA_PATH)

    # # Get user rating
    # get_user_rating(user_id=[1130712, 2536322], user_data_df=user_data_df)

    # Get rating for both users
    get_both_users_rated_movies(user_id1=1130712, user_id2=2536322, user_data_df=user_data_df)

    # Clean
    spark.stop()
