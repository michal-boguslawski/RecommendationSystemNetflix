import pandas as pd


def filter_top_k(dict_: dict, k: int, desc: bool = True) -> dict:
    return dict(sorted(dict_.items(), key=lambda x: x[1], reverse=desc)[:k])

def remapping(dict_: dict, mapping: dict) -> dict:
    return {mapping[key]: item for key, item in dict_.items()}

def load_movie_mapping(df: pd.DataFrame) -> dict:
    movie_df = df.copy()
    movie_df.set_index("MovieID", inplace=True)
    movie_df = movie_df["Title"]
    return movie_df.to_dict()
