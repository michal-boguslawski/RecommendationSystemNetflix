import os
import pandas as pd
from .collaborative_filtering import InferenceUserBasedCollaborativeFilteringKNN
from ..utils.data import drop_rated_movies


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
MOVIE_DATA_PATH = "data/silver/netflix_movie_data"


class RecommenderService:
    def __init__(self):
        self.model = InferenceUserBasedCollaborativeFilteringKNN()
        self.model.load_model()

    def predict_for_user(
        self,
        user_id: int,
        user_data_df: pd.DataFrame,
    ) -> dict:
        preds = self.model.predict_for_user(
            user_id=user_id,
            user_data_df=user_data_df
        )
        filtered_preds = drop_rated_movies(user_id, user_data_df, preds)
        return filtered_preds
