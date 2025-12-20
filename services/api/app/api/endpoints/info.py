# api/app/api/endpoints/recommend.py
from fastapi import APIRouter, Depends, Query
import pandas as pd
from ..deps import get_users_data, get_movie_mapping
from ...models.info import UsersResponse, ListMoviesResponse

router = APIRouter()

@router.get(
    "/list_users",
    response_model=UsersResponse,
)
async def list_users(
    user_data_df: pd.DataFrame = Depends(get_users_data),
    rated_movies: list[int] | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
):
    print(f"Start listing users")
    # Filter selected movies
    if rated_movies:
        user_data_df = user_data_df[
            user_data_df["MovieID"].isin(rated_movies)
        ]

    # Get unique and deterministric users
    users = (
        user_data_df.index
        .unique()
        .sort_values()
        .to_list()
    )

    # pagination
    start = (page - 1) * page_size
    end = start + page_size
    paginated_users = users[start:end]

    return {
        "users": paginated_users,
        "page": page,
        "page_size": page_size,
        "total_users": len(users),
        "total_pages": (len(users) + page_size - 1) // page_size,
        "has_next": end < len(users),
        "has_prev": start > 0,
    }

@router.get(
    "/list_movies",
    response_model=ListMoviesResponse,
)
async def list_movies(
    movie_mapping: dict = Depends(get_movie_mapping),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
):
    movies = list(movie_mapping.items())
    # pagination
    start = (page - 1) * page_size
    end = start + page_size
    page_items  = movies[start:end]
    return {
        "movies": dict(page_items),
        "page": page,
        "page_size": page_size,
        "total_movies": len(movies),
        "total_pages": (len(movies) + page_size - 1) // page_size,
        "has_next": end < len(movies),
        "has_prev": start > 0,
    }
