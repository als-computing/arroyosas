import os

from tiled.client import from_uri

c = from_uri("http://tiled:8000", api_key=os.getenv("TILED_API_KEY"))
raw = c["bl733/raw"]