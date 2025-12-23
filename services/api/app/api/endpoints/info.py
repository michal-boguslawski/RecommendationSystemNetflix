# api/app/api/endpoints/recommend.py
from fastapi import APIRouter, Depends, Query, HTTPException
import pandas as pd
from ..deps import get_users_data, get_movie_mapping
from ..utils.cache import get_filtered_users
from ...models.info import UsersResponse, ListMoviesResponse
from ...utils.data import paginate_dict

router = APIRouter()

@router.get(
    "/users",
    response_model=UsersResponse,
)
async def list_users(
    user_data_df: pd.DataFrame = Depends(get_users_data),
    movies: list[int] | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
):
    print(f"Start listing users")
    # Filter selected movies
    users = await get_filtered_users(user_data_df, movies)

    # pagination
    start = (page - 1) * page_size
    end = start + page_size
    print(f"End listing users from {start} to {end} for page {page} with size {page_size}")
    
    # Raise error if users is not a list
    if not isinstance(users, list):
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: expected a list of users, got {type(users).__name__}"
        )

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
    "/movies",
    response_model=ListMoviesResponse,
)
async def list_movies(
    movie_mapping: dict = Depends(get_movie_mapping),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
):
    # pagination
    paginated_items = paginate_dict(
        movie_mapping,
        page,
        page_size
    )
    
    return {
        "movies": paginated_items,
        "page": page,
        "page_size": page_size,
        "total_movies": len(movie_mapping),
        "total_pages": (len(movie_mapping) + page_size - 1) // page_size,
        "has_next": page * page_size < len(movie_mapping),
        "has_prev": page > 1,
    }
