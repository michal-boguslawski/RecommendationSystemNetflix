import os
from ..models.collaborative_filtering import TrainingUserBasedCollaborativeFilteringKNN


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data/v1"
USER_STATS_PATH = f"s3a://{BUCKET}/data/gold/user_stats/v1"
USER_NEIGHBORS_PATH = f"s3a://{BUCKET}/data/gold/models/user_based_collaborative_filtering/user_neighbors/v1"


if __name__ == "__main__":
    from ml.utils.spark_utils import get_spark_session
    from ml.features.utils import filter_latest_df_version
    spark = get_spark_session("Train User Based CF")
    user_data_df = spark.read.parquet(USER_DATA_PATH)
    user_stats_df = spark.read.parquet(USER_STATS_PATH)

    user_stats_df = filter_latest_df_version(user_stats_df)

    model = TrainingUserBasedCollaborativeFilteringKNN()
    model.train(user_data_df, user_stats_df)

    model.save_model(USER_NEIGHBORS_PATH)

    spark.stop()