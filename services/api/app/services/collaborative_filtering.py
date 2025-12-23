# api/app/services/recommender.py
import os
import pandas as pd
 
from ..models.base import InferenceModel
from ..utils.predictions import penalize_predictions
from ..utils.s3 import load_pandas_df_from_s3

BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
USER_DATA_PATH = f"data/silver/netflix_user_data/v1"
USER_NEIGHBORS_PATH = f"data/gold/models/user_based_collaborative_filtering/user_neighbors/v1"


class InferenceUserBasedCollaborativeFilteringKNN(InferenceModel):
    def __init__(
        self,
        smoothing_factor: int = 5
    ):
        self.smoothing_factor = smoothing_factor
        self.user_neighbor_corrs_df: pd.DataFrame | None = None

    def _load_user_data(self, user_data_path: str) -> pd.DataFrame:
        user_data_df = load_pandas_df_from_s3(BUCKET, user_data_path)
        user_data_df.set_index("UserID", inplace=True)
        return user_data_df

    def load_model(self, user_neighbor_corrs_path: str = USER_NEIGHBORS_PATH, *args, **kwargs):
        print("Start loading data")
        super().load_model(*args, **kwargs)
        user_neighbor_corrs_df = load_pandas_df_from_s3(BUCKET, user_neighbor_corrs_path)
        user_neighbor_corrs_df["version_date"] = pd.to_datetime(
            user_neighbor_corrs_df["version_date"].astype(str)
        )
        latest_date = user_neighbor_corrs_df["version_date"].max()
        user_neighbor_corrs_df = user_neighbor_corrs_df[
            user_neighbor_corrs_df["version_date"] == latest_date
        ]
        user_neighbor_corrs_df.drop(columns=["version_date"], inplace=True)
        user_neighbor_corrs_df.set_index("user", inplace=True)
        self.user_neighbor_corrs_df = user_neighbor_corrs_df
        print("Finished loading data")

    def predict_for_user(self, user_id: int, user_data_df: pd.DataFrame, *args, **kwargs) -> dict:
        if self.user_neighbor_corrs_df is None:
            raise AttributeError("Missing load")
        user_neighbors_df = self.user_neighbor_corrs_df.loc[user_id]

        neighbors_ratings_df = user_data_df.merge(
            user_neighbors_df,
            how="inner",
            left_index=True,
            right_on="neighbor"
        )

        preds = (
            neighbors_ratings_df
            .assign(weighted_rating=lambda x: x["Rating"] * x["corr"])
            .groupby("MovieID")
            .agg(
                score=("weighted_rating", "sum"),
                corr_sum=("corr", "sum"),
                count=("Rating", "count")
            )
            .assign(
                Rating=lambda x: x["score"] / x["corr_sum"]
            )
        )
        preds = penalize_predictions(preds, self.smoothing_factor)
        return preds[["Rating"]].to_dict(orient="index")
