# api/app/models/recommend.py
from pydantic import BaseModel
from typing import Dict

class MoviesResponse(BaseModel):
    movies: Dict[str, float]
    page: int
    page_size: int
    total_movies: int
    total_pages: int
    has_next: bool
    has_prev: bool
