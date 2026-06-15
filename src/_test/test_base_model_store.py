"""Tests for arroyosas.lse_reduction.base_model_store"""

import pytest

from arroyosas.lse_reduction.base_model_store import BaseModelStore


class ConcreteModelStore(BaseModelStore):
    """Concrete implementation for testing the abstract interface."""

    def store_autoencoder_model(self, model_name: str) -> bool:
        return True

    def store_dimred_model(self, model_name: str) -> bool:
        return True

    def get_autoencoder_model(self) -> str:
        return "ae_model"

    def get_dimred_model(self) -> str:
        return "dimred_model"

    def publish_model_update(self, model_type: str, model_name: str) -> bool:
        return True

    def subscribe_to_model_updates(self, callback):
        callback({"update_type": "test"})

    def get_model_loading_state(self):
        return {"is_loading_model": False, "loading_model_type": None}


class TestBaseModelStore:
    def test_cannot_instantiate_abstract(self):
        with pytest.raises(TypeError):
            BaseModelStore()

    def test_concrete_store_autoencoder_model(self):
        store = ConcreteModelStore()
        assert store.store_autoencoder_model("model_v1") is True

    def test_concrete_store_dimred_model(self):
        store = ConcreteModelStore()
        assert store.store_dimred_model("umap_v1") is True

    def test_concrete_get_autoencoder_model(self):
        store = ConcreteModelStore()
        assert store.get_autoencoder_model() == "ae_model"

    def test_concrete_get_dimred_model(self):
        store = ConcreteModelStore()
        assert store.get_dimred_model() == "dimred_model"

    def test_concrete_publish_model_update(self):
        store = ConcreteModelStore()
        assert store.publish_model_update("autoencoder", "model_v1") is True

    def test_concrete_subscribe_to_model_updates(self):
        store = ConcreteModelStore()
        received = []
        store.subscribe_to_model_updates(lambda x: received.append(x))
        assert len(received) == 1
        assert received[0]["update_type"] == "test"

    def test_concrete_get_model_loading_state(self):
        store = ConcreteModelStore()
        state = store.get_model_loading_state()
        assert state["is_loading_model"] is False
        assert state["loading_model_type"] is None

    def test_abstract_methods_all_declared(self):
        abstract_methods = BaseModelStore.__abstractmethods__
        expected = {
            "store_autoencoder_model",
            "store_dimred_model",
            "get_autoencoder_model",
            "get_dimred_model",
            "publish_model_update",
            "subscribe_to_model_updates",
            "get_model_loading_state",
        }
        assert abstract_methods == expected
