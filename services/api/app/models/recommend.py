# api/app/models/recommend.py
from pydantic import BaseModel
from typing import Dict

class RecommendationResponse(BaseModel):
    preds: Dict[str, float]
