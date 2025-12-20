from abc import ABC, abstractmethod


class TrainingModel(ABC):
    @abstractmethod
    def train(self, *args, **kwargs):
        pass

    @abstractmethod
    def save_model(self, path: str, *args, **kwargs):
        pass
