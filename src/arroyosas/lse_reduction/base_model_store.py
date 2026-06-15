from abc import ABC, abstractmethod


class BaseModelStore(ABC):
    @abstractmethod
    def store_autoencoder_model(self, model_name: str) -> bool:
        pass

    @abstractmethod
    def store_dimred_model(self, model_name: str) -> bool:
        pass

    @abstractmethod
    def get_autoencoder_model(self) -> str:
        pass

    @abstractmethod
    def get_dimred_model(self) -> str:
        pass

    @abstractmethod
    def publish_model_update(self, model_type: str, model_name: str) -> bool:
        pass

    @abstractmethod
    def subscribe_to_model_updates(self, callback):
        pass

    @abstractmethod
    def get_model_loading_state(self):
        pass
