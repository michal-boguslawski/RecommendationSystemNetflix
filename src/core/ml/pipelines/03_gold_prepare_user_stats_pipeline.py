import os
from ..features.build_features import compute_user_global_stats
from ..utils.spark_utils import get_spark_session


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data/v1"
USER_STATS_PATH = f"s3a://{BUCKET}/data/gold/user_stats/v1"


if __name__ == "__main__":
    # Start spark session
    spark = get_spark_session()

    # Read data and compute statistics
    user_data_df = spark.read.parquet(USER_DATA_PATH)
    compute_user_global_stats(user_data_df, USER_STATS_PATH)

    # Clean
    spark.stop()