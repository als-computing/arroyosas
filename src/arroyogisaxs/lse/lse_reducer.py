import glob
import importlib
import logging
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import torch
import torchvision.transforms as transforms
from PIL import Image

from ..config import settings as default_settings
from ..schemas import GISAXSRawEvent

logger = logging.getLogger(__name__)


class LatentSpaceReducer:
    """
    Responsible for taking an image, encoding it into a
    latent space, and saving the latent space to a Tiled dataset.
    The encoding is down two ways. First, it's through
    a CNN autoencoder.
    Second, it's through a dimension reduction
    agorithm to a 2D space. The results are saved to a Tiled dataset.
    """

    def __init__(self, settings):
        # Check for CUDA else use CPU
        # needs to be members of the reducer class
        if torch.cuda.is_available():
            device = torch.device("cuda")
            logger.info(f"Using CUDA: {torch.cuda.get_device_name(0)}")
        else:
            device = torch.device("cpu")
            logger.info("Using CPU")
        self.device = device
        self.settings = settings
        self.model_cache = {}
        self.current_ls_model = self.get_ls_model()
        self.curent_dim_reduction_model = self.get_dim_reduction_model()
        self.current_transform = self.get_transform()

    def reduce(self, message: GISAXSRawEvent):
        # 1. Encode the image into a latent space. For now we assume
        pil = Image.fromarray(message.image.array)
        tensor = self.current_transform(pil)
        ls_is_on_gpu = False
        if (
            self.device == torch.device("cuda")
            and self.current_ls_model["config"].type == "torch"
        ):
            ls_is_on_gpu = True

        tensor = tensor.unsqueeze(0).to(self.device)
        latent_space = self.current_torch_model["model"].encoder(tensor)

        # 2. Reduce the latent space to a 2D space
        if self.curent_dim_reduction_model["config"].type == "joblib":
            if ls_is_on_gpu:
                latent_space = latent_space.cpu().detach().numpy()
            else:
                latent_space = latent_space.detach().numpy()
            f_vec = self.curent_dim_reduction_model["model"].transform(latent_space)
        else:  # it's torch
            f_vec = self.current_torch_model.encoder(latent_space)

        return f_vec

    def get_ls_model(self):
        current_name = self.settings.current_latent_space
        return self.get_model(current_name)

    def get_dim_reduction_model(self):
        current_name = self.settings.current_dim_reduction
        return self.get_model(current_name)

    def get_model(self, name: str):
        # check for model in configured models
        for model_config in self.settings.models:
            if model_config.name == name:
                current_model = model_config
                break
        if model_config is None:
            raise ValueError(f"Current model {name} is not found in models")

        # check if model is in cache. If not, load it and add to cache
        if current_model.name not in self.model_cache:
            loaded_model = self.load_model(model_config)
            self.model_cache[current_model.name] = {
                "model": loaded_model,
                "config": model_config,
            }
        return self.model_cache[current_model.name]

    def load_model(self, model_config):
        if model_config.type == "torch":
            model = self.load_torch_model(model_config)
            model = model().to(self.device)
            return model
        return joblib.load(model_config.file)

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

    def import_module_from_path(self, module_name, file_path):
        file_path = Path(file_path).resolve()  # Get absolute path
        module_name = file_path.stem  # Get filename without extension as module name
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if not spec or not spec.loader:
            raise ImportError(f"Cannot load module from {file_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    def load_torch_model(self, model_config: dict) -> torch.nn.Module:
        module = self.import_module_from_path(
            model_config.python_class, model_config.python_file
        )
        model = getattr(module, model_config.python_class)

        # Load the .npz file
        npz_data = np.load(model_config["state_dict"], allow_pickle=True)

        # Convert NumPy arrays to PyTorch tensors
        state_dict = {key: torch.tensor(value) for key, value in npz_data.items()}

        # Load the state dictionary into the model
        model().load_state_dict(state_dict)
        return model

    def import_torch_models(self):
        torch_models = []
        model_dir = self.settings.models.model_dir
        py_files = glob.glob(os.path.join(model_dir, "**/*.py"), recursive=True)

        for py_file in py_files:
            module_name = os.path.splitext(os.path.relpath(py_file, model_dir))[
                0
            ].replace(os.sep, ".")
            module = importlib.import_module(module_name)
            for name, obj in module.__dict__.items():
                if isinstance(obj, type) and issubclass(obj, torch.nn.Module):
                    logger.info(f"Found torch model: {name} in {module_name}")
                    torch_models.append(obj)
        return torch_models

    @classmethod
    def with_models_loaded(cls, settings) -> "LatentSpaceReducer":
        if settings is None:
            settings = default_settings
        reducer = cls(settings)
        # Load the models now
        reducer.current_torch_model = reducer.get_ls_model()
        reducer.curent_dim_reduction_model = reducer.get_dim_reduction_model()
        reducer.current_transform = reducer.get_transform()

        return reducer


if __name__ == "__main__":
    reducer = LatentSpaceReducer.with_models_loaded(default_settings.lse)
    reducer.reduce()
