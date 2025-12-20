from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseModel(ABC):
    @abstractmethod
    def train(self, *args, **kwargs):
        pass

    @abstractmethod
    def predict(self, user_ratings: dict | DataFrame, *arg, **kwargs) -> dict | None:
        pass

    @abstractmethod
    def predict_user(self, user_id: int, *arg, **kwargs) -> dict | None:
        pass

    @abstractmethod
    def batch_predict(self, list_ratings: list[dict], *args, **kwargs) -> list[dict] | None:
        pass


class TrainingModel(ABC):
    @abstractmethod
    def train(self, *args, **kwargs):
        pass

    @abstractmethod
    def save_model(self, path: str, *args, **kwargs):
        pass
