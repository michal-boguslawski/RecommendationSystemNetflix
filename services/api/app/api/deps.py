# api/app/api/deps.py
from fastapi import Request
import os
import pandas as pd
from ..services.recommender import RecommenderService
from ..utils.s3 import load_pandas_df_from_s3
from ..utils.data import load_movie_mapping


BUCKET = os.getenv("MINIO_BUCKET_NAME", "recommendation-system")
MOVIE_DATA_PATH = "data/silver/netflix_movie_data"
USERS_DATA_PATH = "data/silver/netflix_user_data"


def init_resources():
    from services.api.app.main import app
    print("Start intitialization of FastApi application")

    recommender = RecommenderService()
    app.state.recommender = recommender

    movie_data_df = load_pandas_df_from_s3(BUCKET, MOVIE_DATA_PATH)
    movie_mapping = load_movie_mapping(movie_data_df)
    app.state.movie_mapping = movie_mapping

    user_data_df = load_pandas_df_from_s3(BUCKET, USERS_DATA_PATH)
    user_data_df.set_index("UserID", inplace=True)
    app.state.users_data = user_data_df
    print("Finish intitialization of FastApi application")

def get_recommender(request: Request) -> RecommenderService:
    return request.app.state.recommender

def get_movie_mapping(request: Request) -> dict:
    return request.app.state.movie_mapping

def get_users_data(request: Request) -> pd.DataFrame:
    return request.app.state.users_data
