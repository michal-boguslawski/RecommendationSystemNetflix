import os
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from .base import BaseModel
from ml.features.utils import normalize_ratings_df
from ml.utils.data_utils import filter_user


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"s3a://{BUCKET}/data/silver/netflix_user_data.parquet"
USER_STATS_PATH = f"s3a://{BUCKET}/data/gold/user_stats/"


class UserBasedCFkNN(BaseModel):
    """Collaborative Filtering User Based with k-nearest neighbours"""
    def __init__(
        self,
        spark: SparkSession,
        k: int = 50,
        min_corated_movies: int = 10,
        C: int = 5
    ):
        super().__init__()

        self.spark = spark

        self.k = k
        self.min_corated_movies = min_corated_movies
        self.C = C
        
        self._user_data_df: DataFrame | None = None
        self._user_stats_df: DataFrame | None = None
        
        self._load_dfs()

    def _load_dfs(self):
        """Load dataframes"""
        _user_data_df = self.spark.read.parquet(USER_DATA_PATH)
        _user_data_df = _user_data_df.cache()
        self._user_data_df = _user_data_df

        self._user_stats_df = self.spark.read.parquet(USER_STATS_PATH)

    def _find_k_nearest_neighbours(self, all_user_data_df: DataFrame, user_data_df: DataFrame) -> DataFrame:
        """Find k nearest neighbours of user_id"""

        # Join user data with all user data to find neighbours
        pairs = (
            user_data_df.alias("user")
            .join(
                all_user_data_df.alias("neighbours"),
                ( F.col("user.MovieID") == F.col("neighbours.MovieID") )
            )
            .filter(
                ( F.col("user.UserID") != F.col("neighbours.UserID") )
            )
            .select(
                F.col("neighbours.MovieID").alias("movie_id"),
                F.col("neighbours.UserID").alias("neighbour"),
                F.col("neighbours.Rating").alias("neighbour_rating"),
                F.col("user.Rating").alias("user_rating")
            )
        )
        
        # Calculate correlations between user and neighbours
        user_corr_df = (
            pairs
            .groupBy("neighbour")
            .agg(
                F.corr(
                    F.col("user_rating"),
                    F.col("neighbour_rating")
                ).alias("corr"),
                F.count("*").alias("corated_movies")
            )
        )
        user_corr_df = user_corr_df.cache()

        # Filted k nearest neighbours
        neighbours = (
            user_corr_df
            .filter(
                ( F.col("corated_movies") >= F.lit(self.min_corated_movies) )
                & ( F.col("corr").isNotNull() )
            )
            .orderBy(F.col("corr").desc())
            .limit(self.k)
        )
        
        return neighbours

    def _find_neighbours_ratings(self, neighbours_df: DataFrame) -> DataFrame:
        """Find ratings of neighbours for movies not rated by user"""
        neighbours_ratings_df = (
            neighbours_df
            .join(
                self._user_data_df.alias("all_users"),
                ( F.col("neighbour") == F.col("all_users.UserID") )
            )
            .select(
                F.col("all_users.UserID").alias("user_id"),
                F.col("all_users.MovieID").alias("movie_id"),
                F.col("all_users.Rating").alias("neighbour_rating"),
                F.col("corr").alias("corr"),
            )
        )

        return neighbours_ratings_df

    @staticmethod
    def _calculate_predictions(neighbours_ratings_df: DataFrame) -> DataFrame:
        """Calculate predictions for movies not rated by user"""
        predictions_df = (
            neighbours_ratings_df
            .groupBy("movie_id")
            .agg(
                F.sum(
                    F.col("neighbour_rating") * F.col("corr")
                ).alias("sum_corr"),
                F.sum(
                    F.abs(F.col("corr"))
                ).alias("sum_abs_corr"),
                F.count("*").alias("n_corr")
            )
            .withColumn(
                "prediction",
                F.col("sum_corr") / F.col("sum_abs_corr")
            )
            .select(
                F.col("movie_id"),
                F.col("prediction"),
                F.col("n_corr").alias("n_corr")
            )
        )

        return predictions_df

    def _adjust_predictions(self, predictions_df: DataFrame) -> DataFrame:
        """Adjust predictions based on number of rated movies by neighbours"""
        adjusted_predictions_df = (
            predictions_df
            .withColumns(
                {
                    "prediction": (
                        F.col("prediction") * F.col("n_corr") / (F.col("n_corr") + self.C)
                    ),
                    "raw_prediction": F.col("prediction")
                }
            )
            .select(
                F.col("movie_id"),
                F.col("prediction"),
                F.col("raw_prediction"),
            )
        )

        return adjusted_predictions_df

    @staticmethod
    def _convert_df(df: DataFrame, convert_to: str = "dict") -> dict:
        """Convert dataframe to dict"""
        if convert_to == "dict":
            return {
                row["movie_id"]: row["prediction"]
                for row in df.collect()
            }
        else:
            raise ValueError("convert_to must be 'dict'")

    def predict_user(self, user_id: int, adjust_by_n_corr: bool = True) -> dict:
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
        if self._user_data_df is None or self._user_stats_df is None:
            raise RuntimeError("Model data not initialized")

        # Normalize data
        normalized_user_data = normalize_ratings_df(self._user_data_df, self._user_stats_df)
        normalized_user_data = normalized_user_data.select("UserID", "MovieID", "Rating")
        normalized_user_data = normalized_user_data.cache()
        
        # Filter user data
        user_data_df = filter_user(all_user_data_df=normalized_user_data, user_id=user_id)
        user_data_df = user_data_df.cache()
        
        # Find k nearest neighbours
        neighbours_df = self._find_k_nearest_neighbours(all_user_data_df=normalized_user_data, user_data_df=user_data_df)

        # Find rating of the neighbours
        neighbours_ratings_df = self._find_neighbours_ratings(neighbours_df=neighbours_df)

        # Calculate predictions
        predictions_df = self._calculate_predictions(neighbours_ratings_df=neighbours_ratings_df)

        # Adjust by number of rated movies by neighbours
        if adjust_by_n_corr:
            predictions_df = self._adjust_predictions(predictions_df=predictions_df)

        # # Convert to dict
        final_predictions_dict = self._convert_df(df=predictions_df)

        return final_predictions_dict

    def batch_predict(self, ratings: list[dict]) -> list[dict]:
        pass

if __name__ == "__main__":
    from ml.utils.spark_utils import get_spark_session
    spark = get_spark_session()
    model = UserBasedCFkNN(spark=spark)
    preds = model.predict_user(923084)
    print(preds)
    spark.stop()
