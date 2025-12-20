# api/app/models/recommend.py
from pydantic import BaseModel
from typing import Dict

class MoviesResponse(BaseModel):
    movies: Dict[str, float]
