# api/app/api/endpoints/recommend.py
from fastapi import APIRouter, Depends, Query, HTTPException
from ..deps import get_recommender, get_movie_mapping, get_users_data
from ..utils.cache import get_user_movies, get_recommendations
from ...services.recommender import RecommenderService
from ...models.movies import MoviesResponse
from ...utils.data import remapping, order_dict, paginate_dict
import pandas as pd

router = APIRouter()

@router.get(
    "/recommend/{user_id}",
    response_model=MoviesResponse,
)
async def recommend(
    user_id: int,
    recommender: RecommenderService = Depends(get_recommender),
    user_data_df: pd.DataFrame = Depends(get_users_data),
    movie_mapping: dict = Depends(get_movie_mapping),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
):
    print(f"Start recommendation for {user_id}")
    recs = await get_recommendations(recommender, user_data_df, user_id)
    # Raise error if users is not a list
    if not isinstance(recs, dict):
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: expected a list of users, got {type(recs).__name__}"
        )

    preds = order_dict(recs, True)

    paginated_preds = paginate_dict(preds, page, page_size)

    remapped_preds = remapping(paginated_preds, movie_mapping)
    
    print(remapped_preds)
    print("Finish recommendation")
    return {
        "movies": remapped_preds,
        "page": page,
        "page_size": page_size,
        "total_movies": len(recs),
        "total_pages": (len(recs) + page_size - 1) // page_size,
        "has_next": page * page_size < len(recs),
        "has_prev": page > 1
    }

@router.get(
    "/movies/{user_id}",
    response_model=MoviesResponse,
)
async def list_movies(
    user_id: int,
    user_data_df: pd.DataFrame = Depends(get_users_data),
    movie_mapping: dict = Depends(get_movie_mapping),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
):
    print(f"Get list of movies rated by {user_id}")
    user_movies = await get_user_movies(user_data_df, user_id)

    # Raise error if users is not a list
    if not isinstance(user_movies, dict):
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: expected a list of users, got {type(user_movies).__name__}"
        )

    # Apply pagination
    paginated_user_movies = paginate_dict(
        user_movies,
        page,
        page_size
    )

    user_movies_dict = remapping(
        paginated_user_movies,
        movie_mapping
    )
    print("Finish getting list of movies")
    print(user_movies_dict)
    return {
        "movies": user_movies_dict,
        "page": page,
        "page_size": page_size,
        "total_movies": len(user_movies),
        "total_pages": (len(user_movies) + page_size - 1) // page_size,
        "has_next": page * page_size < len(user_movies),
        "has_prev": page > 1
    }
    
