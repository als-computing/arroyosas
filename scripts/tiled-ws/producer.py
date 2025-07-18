import time

from bluesky import RunEngine
from bluesky.callbacks.tiled_writer import TiledWriter
from bluesky.plans import count
from ophyd.sim import img
from tiled.client import from_uri

RE = RunEngine()
client = from_uri("http://localhost:8000", api_key="secret")
tw = TiledWriter(client, batch_size=1)
RE.subscribe(tw)

while True:
    RE(count([img], 10, delay=5))
    time.sleep(5)
