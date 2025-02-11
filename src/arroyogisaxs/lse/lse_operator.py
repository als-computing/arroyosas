import asyncio
import glob
import importlib
import logging
import os
from typing import Type, TypeVar

import joblib
import torch
import torchvision.transforms as transforms
from arroyopy.operator import Operator

from ..config import settings
from ..schemas import GISAXSMessage, GISAXSRawEvent, GISAXSRawStart, GISAXSRawStop

# from tiled.client import from_uri
# from tiled_utils import write_results


DATA_TILED_URI = os.getenv("DATA_TILED_URI", "")
DATA_TILED_KEY = os.getenv("DATA_TILED_KEY", None)
RESULTS_TILED_URI = os.getenv("RESULTS_TILED_URI", "")
RESULTS_TILED_API_KEY = os.getenv("RESULTS_TILED_API_KEY", None)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="Reducer")


class Reducer:
    def __init__(self):
        self.current_torch_model = None
        self.curent_dim_reduction_model = None
        self.current_transform = None
        self.loaded_torch_models = {}  # cache models as they're accessed
        self.loaded_dim_reduction_models = {}  # cache models as they're accessed
        # Check for CUDA else use CPU
        # needs to be members of the reducer class
        if torch.cuda.is_available():
            device = torch.device("cuda")
            logger.info(f"Using CUDA: {torch.cuda.get_device_name(0)}")
        else:
            device = torch.device("cpu")
            logger.info("Using CPU")
        self.device = device

    def extract_ls(self, message: GISAXSRawEvent):
        # 1. Encode the image into a latent space
        tensor = self.current_transform(
            message.image.array
        )  # Add batch and channel dimensions
        f_vec_nn = self.current_torch_model.module.encoder(tensor)
        # 2. Dimension Reduction (PCA)
        f_vec = self.curent_dim_reduction_model.transform(f_vec_nn)
        return f_vec

    def get_torch_model(self):
        current_config = settings.models.torch.current
        for model_config in settings.models.torch.models:
            if model_config.name == current_config:
                current_model = model_config
                break
        if model_config is None:
            raise ValueError(f"Current model {current_config} is not found in models")
        if current_model.name in self.loaded_torch_models:
            return self.loaded_torch_models[current_model]
        else:
            model = load_torch_model(current_model)
            model = model.to(self.device)
            self.loaded_torch_models[current_model] = model
            return model

    def get_dim_reduction_model(self):
        current_model = settings.models.dim_reduction.current
        if current_model in self.loaded_dim_reduction_models:
            return self.loaded_dim_reduction_models[current_model]
        else:
            dim_reduction_model = joblib.load(settings.models.dim_reduction_model.dir)
            self.loaded_dim_reduction_models[current_model] = dim_reduction_model
            return dim_reduction_model

    def get_transform(self):
        return transforms.Compose(
            [
                transforms.Resize(
                    (128, 128)
                ),  # Resize to smaller dimensions to save memory
                transforms.ToTensor(),  # Convert image to PyTorch tensor (0-1 range)
                transforms.Normalize(
                    (0.0,), (1.0,)
                ),  # Normalize tensor to have mean 0 and std 1
            ]
        )

    @classmethod
    def from_settings(cls: Type[T]) -> T:
        reducer = cls()
        # Load the models now
        reducer.current_torch_model = reducer.get_torch_model()
        reducer.curent_dim_reduction_model = reducer.get_dim_reduction_model()
        reducer.current_transform = reducer.get_transform()

        return reducer


class LatentSpaceOperator(Operator):
    """
    Responsible for taking an image, encoding it into a
    latent space, and saving the latent space to a Tiled dataset.
    The encoding is down two ways. First, it's through
    a CNN autoencoder.
    Second, it's through a dimension reduction
    agorithm to a 2D space. The results are saved to a Tiled dataset.
    """

    def __init__(self, reducer: Reducer):
        super().__init__()
        self.reducer = reducer

    async def process(self, message: GISAXSMessage) -> None:
        if isinstance(message, GISAXSRawStart):
            await self.publish(message)
        elif isinstance(message, GISAXSRawEvent):
            await asyncio.to_thread(self.extract_ls, message)
            await self.publish(message)
        elif isinstance(message, GISAXSRawStop):
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None


def load_torch_model(model_config: dict) -> torch.nn.Module:
    module_name, class_name = model_config.python_class.rsplit(".", 1)
    module = importlib.import_module(module_name)
    model_class = getattr(module, class_name)
    model = model_class()
    checkpoint = torch.load(settings.models.torch.model.checkpoint_dir)
    model.load_state_dict(checkpoint["model_state_dict"], strict=False)
    return model


def import_torch_models():
    torch_models = []
    model_dir = settings.models.model_dir
    py_files = glob.glob(os.path.join(model_dir, "**/*.py"), recursive=True)

    for py_file in py_files:
        module_name = os.path.splitext(os.path.relpath(py_file, model_dir))[0].replace(
            os.sep, "."
        )
        module = importlib.import_module(module_name)
        for name, obj in module.__dict__.items():
            if isinstance(obj, type) and issubclass(obj, torch.nn.Module):
                logger.info(f"Found torch model: {name} in {module_name}")
                torch_models.append(obj)
    return torch_models


if __name__ == "__main__":
    reducer = Reducer.from_settings()
    operator = LatentSpaceOperator(reducer)
    operator.run()
