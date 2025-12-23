from fastapi_cache.decorator import cache
from fastapi.encoders import jsonable_encoder
import pandas as pd
from ...services.recommender import RecommenderService


@cache(expire=300)  # cache for 5 minutes
async def get_filtered_users(
    user_data_df: pd.DataFrame,
    movies: list[int] | None = None
) -> list[int]:
    """
    Returns unique, sorted user list filtered by rated_movies.
    Only recomputes if user_data_df or movies change.
    """
    if movies:
        # Filter only the movies we care about
        df_filtered = user_data_df[user_data_df["MovieID"].isin(movies)]
        
        # Count how many unique movies each UserID has from the list
        user_counts = df_filtered.groupby("UserID")["MovieID"].nunique()

        # Users who have all movies
        users_with_all = user_counts[user_counts == len(movies)]
    else:
        users_with_all = user_data_df

    users = users_with_all.index.unique().sort_values().to_list()
    return users

@cache(expire=300)  # cache for 5 minutes
async def get_user_movies(
    user_data_df: pd.DataFrame,
    user_id: int,
) -> dict[int, int]:
    """
    Returns user's rated movies.
    Only recomputes if user_data_df or user_id change.
    """
    user_movies = user_data_df.loc[user_id]
    user_movies.set_index("MovieID", inplace=True)
    return jsonable_encoder(user_movies[["Rating"]].to_dict(orient="index")) # type: ignore

@cache(expire=300)
async def get_recommendations(
    recommender: RecommenderService,
    user_data_df: pd.DataFrame,
    user_id: int
) -> dict:
    recs = recommender.predict_for_user(
        user_id=user_id,
        user_data_df=user_data_df,
    )
    return recs
