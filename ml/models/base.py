from abc import ABC, abstractmethod


class BaseModel(ABC):
    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def predict(self, ratings: dict) -> dict:
        pass

    @abstractmethod
    def predict_user(self, user_id: int) -> dict:
        pass

    @abstractmethod
    def batch_predict(self, list_ratings: list[dict]) -> list[dict]:
        pass
