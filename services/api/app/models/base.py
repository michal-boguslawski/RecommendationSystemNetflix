from abc import ABC, abstractmethod


class InferenceModel(ABC):
    @abstractmethod
    def load_model(self, *args, **kwargs):
        pass

    @abstractmethod
    def predict_for_user(self, user_id: int, *args, **kwargs) -> dict:
        pass
