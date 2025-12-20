# api/app/api/endpoints/recommend.py
from fastapi import APIRouter, Depends, Query
from ..deps import get_recommender, get_movie_mapping, get_users_data
from ...services.recommender import RecommenderService
from ...models.recommend import RecommendationResponse
import pandas as pd

router = APIRouter()

@router.get(
    "/recommend/{user_id}",
    response_model=RecommendationResponse,
)
async def recommend(
    user_id: int,
    k: int = Query(10, ge=1, le=100),
    recommender: RecommenderService = Depends(get_recommender),
    user_data_df: pd.DataFrame = Depends(get_users_data),
    movie_mapping: dict = Depends(get_movie_mapping),
):
    print(f"Start recommendation for {user_id}")
    recs = recommender.predict_for_user(
        user_id=user_id,
        user_data_df=user_data_df,
        movie_mapping=movie_mapping,
        k=k,
    )
    print("Finish recommendation")
    return {"preds": recs}
