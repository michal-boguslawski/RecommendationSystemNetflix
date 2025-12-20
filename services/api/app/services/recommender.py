import os
import pandas as pd
from .collaborative_filtering import InferenceUserBasedCollaborativeFilteringKNN
from ..utils.data import filter_top_k, remapping, filter_out_rated_movies


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
MOVIE_DATA_PATH = "data/silver/netflix_movie_data"


class RecommenderService:
    def __init__(self):
        self.model = InferenceUserBasedCollaborativeFilteringKNN()
        self.model.load_model()

    def predict_for_user(self, user_id: int, user_data_df: pd.DataFrame, movie_mapping: dict, k: int = 10) -> dict:
        preds = self.model.predict_for_user(
            user_id=user_id,
            user_data_df=user_data_df
        )
        filtered_preds = filter_out_rated_movies(user_id, user_data_df, preds)
        top_preds = filter_top_k(filtered_preds, k, True)
        remapped_preds = remapping(top_preds, movie_mapping)
        return remapped_preds
