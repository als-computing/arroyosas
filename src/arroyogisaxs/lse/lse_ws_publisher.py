import asyncio
import json
import logging
from typing import Union

# import msgpack
# import numpy as np
import websockets
from arroyopy.publisher import Publisher

from ..schemas import GISAXSLatentSpaceEvent, GISAXSStart, GISAXSStop

logger = logging.getLogger(__name__)


class LSEWSResultPublisher(Publisher):
    """
    A publisher class for sending dimensionality reduction information

    """

    websocket_server = None
    connected_clients = set()
    current_start_message = None

    def __init__(self, host: str = "localhost", port: int = 8001):
        super().__init__()
        self.host = host
        self.port = port

    async def start(
        self,
    ):
        # Use partial to bind `self` while matching the expected handler signature
        server = await websockets.serve(
            self.websocket_handler,
            self.host,
            self.port,
        )
        logger.info(f"Websocket server started at ws://{self.host}:{self.port}")
        await server.wait_closed()

    async def publish(self, message: GISAXSLatentSpaceEvent) -> None:
        if self.connected_clients:  # Only send if there are clients connected
            asyncio.gather(
                *(self.publish_ws(client, message) for client in self.connected_clients)
            )

    async def publish_ws(
        self,
        client: websockets.ServerConnection,
        message: Union[GISAXSLatentSpaceEvent | GISAXSStart | GISAXSStop],
    ) -> None:
        if isinstance(message, GISAXSStop):
            logger.info(f"WS Sending Stop {message}")
            self.current_start_message = None
            await client.send(json.dumps(message.model_dump()))
            return

        if isinstance(message, GISAXSStart):
            self.current_start_message = message
            logger.info(f"WS Sending Start {message}")
            await client.send(json.dumps(message.model_dump()))
            return

        if isinstance(message, GISAXSLatentSpaceEvent):
            # send image data separately to client memory issues
            message = {
                "tiled_uri": "foo",
                "index": 0,
                "feature_vector": message.feature_vector,
            }
            await client.send(json.dumps(message))

    async def websocket_handler(self, websocket):
        logger.info(f"New connection from {websocket.remote_address}")
        # if websocket.request.path != "/viz":
        #     logger.info(f"Invalid path: {websocket.request.path}, we only support /viz")
        #     return
        self.connected_clients.add(websocket)
        try:
            # Keep the connection open and do nothing until the client disconnects
            await websocket.wait_closed()
        finally:
            # Remove the client when it disconnects
            self.connected_clients.remove(websocket)
            logger.info("Client disconnected")

    @classmethod
    def from_settings(cls, settings: dict) -> "LSEWSResultPublisher":
        return cls(settings.host, settings.port)


# async def test_client(publisher: OneDWSResultPublisher, num_frames: int = 10):
#     import time

#     import pandas as pd
#     from arroyopy.schemas import DataFrameModel, NumpyArrayModel

#     from arroyogisaxs.schemas import GISAXSLatentSpaceEvent, GISAXSStart, GISAXSStop

#     await asyncio.sleep(2)
#     for y in range(100):
#         await publisher.publish(GISAXSStart())
#         for x in range(num_frames):
#             await asyncio.sleep(1)
#             # Create a test pattern image that changes slightly each time
#             frame_number = int(time.time()) % 100  # Change pattern every second
#             image = np.zeros((100, 100), dtype=np.float32)
#             np.fill_diagonal(image, frame_number % 255)

#             # Create a 1D sine wave pattern
#             x = np.linspace(0, 2 * np.pi, 100)
#             one_d_reduction = pd.DataFrame(
#                 {"q": x, "qy": np.sin(x + frame_number * 0.1)}
#             )
#             image_info = {
#                 "frame_number": frame_number,
#                 "width": image.shape[1],
#                 "height": image.shape[0],
#                 "data_type": "uint8",
#             }

#             # Create GISAXSResult message
#             message = GISAXSLatentSpaceEvent()

#             await publisher.publish(message)
#         await publisher.publish(GISAXSStop(num_frames=num_frames))


# async def main(publisher: OneDWSResultPublisher):
#     await asyncio.gather(publisher.start(), test_client(publisher))


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     publisher = GISAXSWSResultPublisher(host="0.0.0.0", port=8001)
#     asyncio.run(main(publisher))
