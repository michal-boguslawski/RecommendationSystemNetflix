import pandas as pd


def order_dict(dict_: dict, desc: bool = True) -> dict:
    return dict(sorted(dict_.items(), key=lambda x: x[1], reverse=desc))

def filter_top_k(dict_: dict, k: int, desc: bool = True) -> dict:
    return dict(sorted(dict_.items(), key=lambda x: x[1], reverse=desc)[:k])

def remapping(dict_: dict, mapping: dict) -> dict:
    return {mapping[int(key)]: item for key, item in dict_.items()}

def load_movie_mapping(df: pd.DataFrame) -> dict:
    movie_df = df.copy()
    movie_df.set_index("MovieID", inplace=True)
    movie_df = movie_df["Title"]
    return movie_df.to_dict()

def drop_rated_movies(
    user_id: int,
    user_data_df: pd.DataFrame,
    preds_dict: dict
):
    user_movies = user_data_df.loc[user_id]["MovieID"]
    filtered_preds = {movie: preds_dict[movie] for movie in preds_dict if movie not in user_movies}
    return filtered_preds

def paginate_dict(dict_: dict, page: int, page_size: int) -> dict:
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    selected = list(dict_.items())[start_idx:end_idx]
    return dict(selected)
