from pydantic import BaseModel
from typing import List, Dict

class UsersResponse(BaseModel):
    users: List[int]
    page: int
    page_size: int
    total_users: int
    total_pages: int
    has_next: bool
    has_prev: bool

class ListMoviesResponse(BaseModel):
    movies: Dict[int, str]
    page: int
    page_size: int
    total_movies: int
    total_pages: int
    has_next: bool
    has_prev: bool