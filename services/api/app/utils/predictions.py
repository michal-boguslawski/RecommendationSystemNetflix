import pandas as pd


def penalize_predictions(
    df: pd.DataFrame,
    penalty: int,
    min_pred: int = 1,
    max_pred: int = 5
) -> pd.DataFrame:
    """
    Penalize predicted ratings using mean-centered shrinkage based on
    neighborhood support.

    This function applies a confidence-based shrinkage to predicted ratings
    by reducing their deviation from the user's mean rating when the number
    of supporting neighbors is small. Predictions with stronger neighborhood
    support (higher `count`) are penalized less, while those with weaker
    support are pulled closer to the user's average rating.

    The shrinkage factor is defined as:

        count / (count + penalty)

    After shrinkage, predictions are clipped to the valid rating range.

    Assumes that the input DataFrame contains predictions for a single user.

    Args:
        df (pd.DataFrame):
            DataFrame containing predicted ratings for one user. Must include:
                - 'pred': float, raw predicted rating
                - 'count': int, number of neighbors contributing to the prediction

        penalty (int):
            Shrinkage strength parameter (λ). Higher values increase penalization
            for items with low neighbor support.

        min_pred (int, optional):
            Minimum allowed rating after penalization. Default is 1.

        max_pred (int, optional):
            Maximum allowed rating after penalization. Default is 5.

    Returns:
        pd.DataFrame:
            A copy of the input DataFrame with the 'pred' column updated to
            penalized and clipped predictions.

    Notes:
        - This method preserves relative ranking among items with similar
          neighborhood support.
        - For DataFrames containing multiple users, apply this function
          per user (e.g., via groupby).
        - The penalty parameter can be tuned using validation metrics
          such as RMSE or ranking-based measures.
    """
    temp_df = df.copy()

    user_mean = temp_df["pred"].mean()
    deviations = temp_df["pred"] - user_mean

    shrinkage = temp_df["count"] / (temp_df["count"] + penalty)
    penalized_deviations = deviations * shrinkage

    temp_df["pred"] = (user_mean + penalized_deviations).clip(min_pred, max_pred)

    return temp_df
