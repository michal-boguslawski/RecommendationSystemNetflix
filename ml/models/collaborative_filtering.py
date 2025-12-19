from datetime import datetime
import os
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from .base import BaseModel
from ml.features.utils import normalize_ratings_df
from ml.utils.spark_utils import monitor_progress


CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data/v1"
USER_STATS_PATH = f"s3a://{BUCKET}/data/gold/user_stats/v1"
USER_NEIGHBORS_PATH = f"s3a://{BUCKET}/data/gold/models/user_based_collaborative_filtering/user_neighbors/v1"


class UserBasedCollaborativeFilteringKNN(BaseModel):
    """
    Implementation
    """

    def __init__(
        self,
        k: int = 50,
        min_corated_movies: int = 10,
        n_user_blocks: int = 1000,
        min_corr_strength: float  = 0.3
    ):
        super().__init__()
        self.k = k
        self.min_corated_movies = min_corated_movies
        self.n_user_blocks = n_user_blocks
        self.min_corr_strength = min_corr_strength

    def _preprocess_df(self, user_data_df: DataFrame, user_stats_df: DataFrame) -> DataFrame:
        """
        Preprocess the user data and user statistics to normalize ratings.

        This method normalizes the user ratings to have zero mean and unit variance,
        and groups users into blocks to reduce computations.

        Args:
            user_data_df (DataFrame): Spark DataFrame containing user ratings data.
                Expected columns: UserID, MovieID, Rating
            stats_df (DataFrame): Spark DataFrame containing user statistics.
                Contains aggregated statistics about users' rating behavior.

        Returns:
            DataFrame: Normalized user data with additional columns for block and normalized ratings.
        """
        
        user_data_df = user_data_df.select("UserID", "MovieID", "Rating")

        # Normalize each user rating to be normally distributed with mean 0 and 1 std
        normalized_user_df = (
            normalize_ratings_df(user_data_df, user_stats_df)
            .withColumn("block", F.pmod(F.hash("UserID"), F.lit(self.n_user_blocks)))  # Group Users into blocks to reduce computations
            .repartition(self.n_user_blocks, "block", "MovieID")
        )
        return normalized_user_df

    def _calculate_correlations(
        self,
        user_df: DataFrame,
        all_users_df: DataFrame | None = None,
        min_corated_movies: int | None = None
    ) -> DataFrame:
        """
        Calculate correlations between users based on their normalized ratings.

        This method calculates correlations between users based on their normalized ratings.
        It uses a window function to calculate correlations between users within each block,
        and then joins the correlations with the normalized ratings to calculate the correlation
        between users for each pair of users.

        Args:
            user_df (DataFrame): Spark DataFrame containing normalized user ratings.
                Expected columns: UserID, MovieID, Rating, block
            all_users_df (DataFrame, optional): Spark DataFrame containing normalized user ratings
                for all users. If not provided, it will be calculated from `user_df`.

        Returns:
            DataFrame: Spark DataFrame containing correlations between users.
                Columns: user, neighbor, corr
        """
        persisted_df = None
        min_corated_movies = min_corated_movies or self.min_corated_movies

        if "block" not in user_df.columns:
            user_df = user_df.withColumn("block", F.lit(0))

        if all_users_df is None:
            # ONE logical plan
            base_df = (
                user_df
                # .repartition(1000, "block", "UserID")
                .persist(StorageLevel.DISK_ONLY)
            )

            user_df = base_df
            all_users_df = base_df
            persisted_df = all_users_df
        else:
            user_df = F.broadcast(user_df)
            # all_users_df is large, user_df is small
            if "block" not in all_users_df.columns:
                all_users_df = all_users_df.withColumn("block", F.lit(0))

        # Join normalized ratings for same movies for each pair of users inside block
        pairs = (
            user_df.alias("u")
            .join(
                all_users_df.alias("n"),
                ( F.col("u.block") == F.col("n.block") ) &
                ( F.col("u.MovieID") == F.col("n.MovieID") ) &
                ( F.col("u.UserID") < F.col("n.UserID") )
            )
            .select(
                F.col("u.UserID").alias("user"),
                F.col("n.UserID").alias("neighbor"),
                F.col("u.Rating").alias("x"),
                F.col("n.Rating").alias("y")
            )
            .repartition(2048, "user", "neighbor")
        )
        if persisted_df is not None:
            persisted_df.unpersist(blocking=False)

        # Calculate components needed for correlations between users
        stats_df = (
            pairs
            .groupBy("user", "neighbor")
            .agg(
                F.count("*").alias("n"),
                F.sum("x").alias("sum_x"),
                F.sum("y").alias("sum_y"),
                F.sum(F.col("x") * F.col("y")).alias("sum_xy"),
                F.sum(F.col("x") * F.col("x")).alias("sum_x2"),
                F.sum(F.col("y") * F.col("y")).alias("sum_y2"),
            )
            .filter(F.col("n") >= min_corated_movies)  # prune weak correlations early
            .repartition(1024, "user", "neighbor")
        )

        # Calculate correlations
        corrs_df = (
            stats_df
            .withColumn(
                "corr",
                (
                    F.col("sum_xy") - ( F.col("sum_x") * F.col("sum_y") / F.col("n") )
                ) /
                (
                    F.sqrt(F.col("sum_x2") - (F.col("sum_x") ** 2) / F.col("n")) *
                    F.sqrt(F.col("sum_y2") - (F.col("sum_y") ** 2) / F.col("n"))
                )
            )
            .filter(F.col("corr") > self.min_corr_strength)
            .select("user", "neighbor", "corr")
        )

        # Add other direction of correlation for filtering only 50 strongest correlations for every user
        corrs_df = (
            corrs_df
            .unionAll(
                corrs_df
                .select(
                    F.col("neighbor").alias("user"),
                    F.col("user").alias("neighbor"),
                    F.col("corr")
                )
            )
        )

        return corrs_df

    def _filter_k_nearest_neighbors(self, corrs_df: DataFrame) -> DataFrame:
        """
        Keep correlations for only k nearest neighbors
        """
        # Limiting to only strongest correlations for every user
        w = Window.partitionBy("user").orderBy(F.col("corr").desc())
        neighbors = (
            corrs_df
            .withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= self.k)
            .drop("rank")
        )
        return neighbors

    def _write_neighbors_correlations(self, neighbors: DataFrame):
        """
        Write correlations into s3 bucket in parquet format
        """
        neighbors = neighbors.withColumn("version_date", F.lit(CURRENT_DATE))
        neighbors.coalesce(8).write.partitionBy("version_date").mode("append").parquet(USER_NEIGHBORS_PATH)
        print(f"Done! Correlations written in {USER_NEIGHBORS_PATH}")

    def train(self, user_data_df: DataFrame, user_stats_df: DataFrame):
        """
        Train the user-based collaborative filtering kNN model.
        
        This method trains the model by processing user ratings data and user statistics
        to find similar users based on rating patterns.
        
        Args:
            user_data_df (DataFrame): Spark DataFrame containing user ratings data.
                Expected columns: UserID, MovieID, Rating
            stats_df (DataFrame): Spark DataFrame containing user statistics.
                Contains aggregated statistics about users' rating behavior.
                
        Returns:
            None
            
        Note:
            The method modifies the model's internal state by computing user similarities
            and storing nearest neighbors for each user.
        """
        print(20 * "=", "Training model", 20 * "=")
        normalized_user_df = self._preprocess_df(user_data_df, user_stats_df)

        # Calculate correlations between every pair of users within the same block
        corrs_df = self._calculate_correlations(user_df=normalized_user_df)

        # Limiting to only k strongest correlations for every user
        neighbors = self._filter_k_nearest_neighbors(corrs_df)

        # Write correlations into s3 bucket in parquet format
        self._write_neighbors_correlations(neighbors)

        print(20 * "=", "Training model finished", 20 * "=")

    def predict_user(self, user_id: int, user_data_df: DataFrame, user_neigbors_corrs_df: DataFrame) -> dict:
        """
        Predict ratings for a given user based on the trained model.

        This method predicts ratings for a given user by finding similar users
        based on their rating patterns and using their ratings to predict ratings
        for the target user.

        Args:
            user_id (int): ID of the user for whom ratings are to be predicted.
            user_data_df (DataFrame): Spark DataFrame containing user ratings data.
                Expected columns: UserID, MovieID, Rating
            user_neigbors_corrs_df (DataFrame): Spark DataFrame containing correlations between users.
                Expected columns: user, neighbor, corr

        Returns:
            dict: A dictionary containing predicted ratings for the target user.
                Keys are movie IDs, and values are predicted ratings.
        """
        user_neighbors = user_neigbors_corrs_df.filter(F.col("user") == user_id)
        
        neighbors_data = (
            user_data_df.alias("u")
            .join(
                user_neighbors.alias("n"),
                on=F.col("u.UserID") == F.col("n.neighbor"),
                how="inner"
            )
            .select(F.col("u.MovieID"), F.col("u.Rating"), F.col("n.corr"))
        )

        movie_preds = (
            neighbors_data
            .groupBy("MovieID")
            .agg(
                F.sum(F.col("corr") * F.col("Rating")).alias("sum_corr_rating"),
                F.sum(F.abs(F.col("corr"))).alias("sum_abs_corr"),
                F.count("*").alias("n")
            )
            .withColumn("pred", F.col("sum_corr_rating") / F.col("sum_abs_corr"))
        )
        movie_preds = movie_preds.select("MovieID", "pred").collect()
        return {row.MovieID: row.pred for row in movie_preds}

    def predict(self, *arg, **kwargs) -> dict | None:
        return super().predict(*arg, **kwargs)

    def batch_predict(self, list_ratings: list[dict], *args, **kwargs) -> list[dict] | None:
        return super().batch_predict(list_ratings, *args, **kwargs)


if __name__ == "__main__":
    from ml.utils.spark_utils import get_spark_session
    from ml.features.utils import filter_latest_df_version
    spark = get_spark_session("TestCollaborativeFiltering")
    user_data_df = spark.read.parquet(USER_DATA_PATH)
    user_stats_df = spark.read.parquet(USER_STATS_PATH)

    user_stats_df = filter_latest_df_version(user_stats_df)

    model = UserBasedCollaborativeFilteringKNN()
    model.train(user_data_df, user_stats_df)

    # user_data_df = spark.read.parquet(USER_DATA_PATH).persist(StorageLevel.MEMORY_AND_DISK)
    # user_neigbors_corrs_df = spark.read.parquet(USER_NEIGHBORS_PATH).persist(StorageLevel.MEMORY_AND_DISK)
    # preds = model.predict_user(user_id=923084, user_data_df=user_data_df, user_neigbors_corrs_df=user_neigbors_corrs_df)
    # user_neigbors_corrs_df.unpersist()
    # user_data_df.unpersist()
    # print(preds)
