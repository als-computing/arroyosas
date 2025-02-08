import pytest
from ls.lse_operator import import_torch_models
from unnittest import MagicMock


@pytest.fixture
def model():
    return MagicMock()


# def test_latent_space_operator(model):
#     operator = LatentSpaceOperator(model)


def test_load_torch_models():
    import torch

    models = import_torch_models()
    assert len(models) > 0
    for model in models:
        assert issubclass(model, torch.nn.Module)
