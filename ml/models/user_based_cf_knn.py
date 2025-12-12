import os
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from base import BaseModel
from ..utils.spark_utils import get_spark_session


spark = get_spark_session()
BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data.parquet"
USER_USER_CORR_PATH = f"s3a://{BUCKET}/data/gold/user_user_corr/"


class UserBasedCFkNN(BaseModel):
    """Collaborative Filtering User Based with k-nearest neighbours"""
    def __init__(self, k: int = 5):
        super().__init__()
        self.k = k
        self._user_data_df: DataFrame
        self._corr_df: DataFrame
        
        self._load_dfs()

    def _load_dfs(self):
        """Load dataframes"""
        self._user_data_df = spark.read.parquet(USER_DATA_PATH)
        self._corr_df = spark.read.parquet(USER_USER_CORR_PATH)

    def _find_k_nearest_neighbours(self, user_id: int) -> list:
        """Find k nearest neighbours of user_id"""
        # Filter out correlations for selected user
        user_corr_df = self._corr_df.filter(
            ( F.col("user1") == user_id ) |
            ( F.col("user2") == user_id )
        )

        # Order by correlation
        user_corr_df = user_corr_df.orderBy(F.col("corr").desc())

        # Select k nearest neighbours
        user_corr_df = user_corr_df.limit(self.k)

        # Get the "other" user in each pair
        user_corr_df = user_corr_df.withColumn(
            "neighbour",
            F.when(F.col("user1") == user_id, F.col("user2")).otherwise(F.col("user1"))
        )

        # Convert list of Row to list of user_id
        neighbours = [row["neighbour"] for row in user_corr_df.select("neighbour").collect()]
        
        return neighbours

    def _filter_ratings_of_users(self, neighbours: list[int]) -> DataFrame:
        """Filter out ratings of neighbours"""

        # Filter out ratings of neighbours
        neighbours_ratings_df = self._user_data_df.filter(
            F.col("user_id").isin(neighbours)
        )

        return neighbours_ratings_df

    def _compute_average_ratings(self, neighbours_ratings_df: DataFrame) -> DataFrame:
        """Compute average ratings of neighbours"""

        # Compute average ratings of neighbours
        neighbours_ratings_df = neighbours_ratings_df.groupBy("movie_id").agg(
            F.mean("rating").alias("avg_rating")
        )

        return neighbours_ratings_df

    def train(self):
        pass

    def predict(self, ratings: dict[int, int]) -> dict:
        pass

    def predict_user(self, user_id: int) -> dict:
        """
        Predict movie ratings for a user based on k-nearest neighbours.

        Parameters
        ----------
        user_id : int
            ID of the user to predict ratings for.

        Returns
        -------
        dict
            Dictionary mapping `movie_id` to predicted rating based on neighbours.

        Examples
        --------
        >>> recommender.predict_user(12345)
        {101: 4.5, 102: 3.8, 103: 5.0}
        """
        # Find k nearest neighbours
        neighbours = self._find_k_nearest_neighbours(user_id)

        # Filter out ratings of neighbours
        neighbours_ratings_df = self._filter_ratings_of_users(neighbours)

        # Compute average ratings of neighbours
        neighbours_ratings_df = self._compute_average_ratings(neighbours_ratings_df)

        # Convert to dict
        neighbours_ratings = {
            row["movie_id"]: row["avg_rating"]
            for row in neighbours_ratings_df.collect()
        }

        return neighbours_ratings

    def batch_predict(self, ratings: list[dict]) -> list[dict]:
        pass