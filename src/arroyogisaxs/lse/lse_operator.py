import asyncio
import glob
import importlib
import logging
import os
from typing import Type, TypeVar

# import joblib
import torch

# import torchvision.transforms as transforms
from arroyopy.operator import Operator

from ..config import settings
from ..schemas import GISAXSEvent, GISAXSMessage, GISAXSStart, GISAXSStop

# from tiled.client import from_uri
# from tiled_utils import write_results


DATA_TILED_URI = os.getenv("DATA_TILED_URI", "")
DATA_TILED_KEY = os.getenv("DATA_TILED_KEY", None)
RESULTS_TILED_URI = os.getenv("RESULTS_TILED_URI", "")
RESULTS_TILED_API_KEY = os.getenv("RESULTS_TILED_API_KEY", None)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="LatentSpaceOperator")


class LatentSpaceOperator(Operator):
    """
    Responsible for taking an image, encoding it into a
    latent space, and saving the latent space to a Tiled dataset.
    The encoding is down two ways. First, it's through
    a CNN autoencoder.
    Second, it's through a dimension reduction
    agorithm to a 2D space. The results are saved to a Tiled dataset.
    """

    def __init__(self, model: torch.nn, dim_reduction_model, transform):
        super().__init__()

    @classmethod
    def from_settings(cls: Type[T]) -> T:
        module_name, class_name = settings.models.torch_model.python_class.rsplit(
            ".", 1
        )
        module = importlib.import_module(module_name)
        model_class = getattr(module, class_name)
        model = model_class()
        checkpoint = torch.load(settings.models.torch_model.checkpoint_dir)
        model.load_state_dict(checkpoint["model_state_dict"], strict=False)
        # dim_reduction_model = joblib.load(settings.models.dim_reduction_model.dir)

        # Check for CUDA else use CPU
        if torch.cuda.is_available():
            device = torch.device("cuda")
            logger.info(f"Using CUDA: {torch.cuda.get_device_name(0)}")
        else:
            device = torch.device("cpu")
            logger.info("Using CPU")
        model = model.to(device)

        # Define the transformation to apply to the data
        # transform = transforms.Compose(
        #     [
        #         transforms.Resize(
        #             (128, 128)
        #         ),  # Resize to smaller dimensions to save memory
        #         transforms.ToTensor(),  # Convert image to PyTorch tensor (0-1 range)
        #         transforms.Normalize(
        #             (0.0,), (1.0,)
        #         ),  # Normalize tensor to have mean 0 and std 1
        #     ]
        # )
        # operator = cls(model, dim_reduction_model, transform)
        # return operator

    async def process(self, message: GISAXSMessage) -> None:
        if isinstance(message, GISAXSStart):
            await self.publish(message)
        elif isinstance(message, GISAXSEvent):
            await asyncio.to_thread(self.extract_ls, message)
            await self.publish(message)
        elif isinstance(message, GISAXSStop):
            await self.publish(message)
        else:
            logger.warning(f"Unknown message type: {type(message)}")
        return None

    def extract_ls(self, message: GISAXSEvent):
        # 1. Encode the image into a latent space
        tensor = self.transform(message.image.array)  # Add batch and channel dimensions
        f_vec_nn = self.model.module.encoder(tensor)
        # 2. Dimension Reduction (PCA)
        f_vec = self.dim_reduction_model.transform(f_vec_nn)
        return f_vec


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


def main():
    operator = LatentSpaceOperator.from_settings()
    operator.run()
