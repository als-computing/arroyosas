import asyncio
import json
import logging
from typing import Union

import msgpack
import numpy as np
import websockets
from arroyopy.publisher import Publisher

from .schemas import GISAXS1DReduction, GISAXSStart, GISAXSStop, SerializableNumpyArrayModel

logger = logging.getLogger(__name__)


class OneDWSPublisher(Publisher):
    """
    A publisher class for sending XPSResult messages over a web sockets.

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

    async def publish(self, message: GISAXS1DReduction) -> None:
        if self.connected_clients:  # Only send if there are clients connected
            asyncio.gather(
                *(self.publish_ws(client, message) for client in self.connected_clients)
            )

    async def publish_ws(
        self,
        #  client: websockets.client.ClientConnection,
        client,
        message: Union[GISAXS1DReduction | GISAXSStart | GISAXSStop],
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

        # send image data separately to client memory issues
        image_bundle = await asyncio.to_thread(pack_images, message)

        await client.send(image_bundle)

    async def websocket_handler(self, websocket):
        logger.info(f"New connection from {websocket.remote_address}")
        if websocket.request.path != "/viz":
            logger.info(f"Invalid path: {websocket.request.path}, we only support /viz")
            return
        self.connected_clients.add(websocket)
        try:
            # Keep the connection open and do nothing until the client disconnects
            await websocket.wait_closed()
        finally:
            # Remove the client when it disconnects
            self.connected_clients.remove(websocket)
            logger.info("Client disconnected")

    @classmethod
    def from_settings(cls, settings: dict) -> "OneDWSPublisher":
        return cls(settings.host, settings.port)


def convert_to_uint8(image: np.ndarray) -> bytes:
    # Define the desired output range
    output_min = 0.0
    output_max = 1.0

    # Find the minimum and maximum values in the input array
    input_min = np.min(image)
    input_max = np.max(image)

    # Apply contrast stretching
    stretched_array = (image - input_min) / (input_max - input_min)
    stretched_array = stretched_array * (output_max - output_min) + output_min
    image_uint8 = (stretched_array * 255).astype(np.uint8)
    return image_uint8.tobytes()
    # Define the lower and upper percentiles
    # lower_percentile = 2
    # upper_percentile = 98
  
    # # Compute the values at the specified percentiles
    # v_min = np.percentile(image, lower_percentile)
    # v_max = np.percentile(image, upper_percentile)

    # # Apply contrast stretching
    # stretched_array = np.clip(intensity_array, v_min, v_max)
    # stretched_array = (stretched_array - v_min) / (v_max - v_min)
    # image_uint8 = (stretched_array * 255).astype(np.uint8)
    # return image_uint8.tobytes()
#     
# def convert_to_uint8(image: np.ndarray) -> bytes:
#     """
#     Convert an image to uint8, scaling image
#     """
#     # scaled = (image - image.min()) / (image.max() - image.min()) * 255
#     # return scaled.astype(np.uint8).tobytes()

#     image_normalized = (image - image.min()) / (image.max() - image.min())

#     # Apply logarithmic stretch
#     log_stretched = np.log1p log1p(image_normalized)  # log(1 + x) to handle near-zero values

#     # Normalize the log-stretched image to [0, 1] again
#     log_stretched_normalized = (log_stretched - log_stretched.min()) / (
#         log_stretched.max() - log_stretched.min()
#     )

#     # Convert to uint8 (range [0, 255])
#     image_uint8 = (log_stretched_normalized * 255).astype(np.uint8)
#     return image_uint8.tobytes()


def pack_images(message: GISAXS1DReduction) -> bytes:
    """
    Pack all the images into a single msgpack message
    """
    logger.debug(f"Image max {message.raw_frame.array.max()}")
    logger.debug(f"Image avg {message.raw_frame.array.mean()}")
    try:
        return msgpack.packb(
            {
                "raw_frame": convert_to_uint8(message.raw_frame.array),
                "curve": convert_to_uint8(message.curve.array),
                "raw_frame_tiled_url": message.raw_frame_tiled_url,
                "curve_tiled_url": message.curve_tiled_url,
                "width": message.raw_frame.array.shape[1],
                "height": message.raw_frame.array.shape[0],
                "data_type": message.raw_frame.array.dtype.name,
            }
        )
    except Exception as e:
        logger.error(f"Error packing images: {e}")
        raise e


async def test_client(publisher: OneDWSPublisher, num_frames: int = 10):
    import time

    import pandas as pd
    from arroyopy.schemas import DataFrameModel, NumpyArrayModel

    from arroyogisaxs.schemas import (
        GISAXSImageInfo,
        GISAXSRawEvent,
        GISAXSStart,
        GISAXSStop,
    )

    await asyncio.sleep(2)
    for y in range(100):
        await publisher.publish(GISAXSStart())
        for x in range(num_frames):
            await asyncio.sleep(1)
            # Create a test pattern image that changes slightly each time
            frame_number = int(time.time()) % 100  # Change pattern every second
            image = np.zeros((100, 100), dtype=np.float32)
            np.fill_diagonal(image, frame_number % 255)

            # Create a 1D sine wave pattern
            x = np.linspace(0, 2 * np.pi, 100)
            one_d_reduction = pd.DataFrame(
                {"q": x, "qy": np.sin(x + frame_number * 0.1)}
            )
            image_info = {
                "frame_number": frame_number,
                "width": image.shape[1],
                "height": image.shape[0],
                "data_type": "uint8",
            }

            # Create GISAXSResult message
            message = GISAXSRawEvent(
                image_info=GISAXSImageInfo(**image_info),
                image=NumpyArrayModel(array=image),
                one_d_reduction=DataFrameModel(df=one_d_reduction),
            )

            await publisher.publish(message)
        await publisher.publish(GISAXSStop(num_frames=num_frames))


async def main(publisher: OneDWSPublisher):
    await asyncio.gather(publisher.start(), test_client(publisher))


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     publisher = GISAXSWSResultPublisher(host="0.0.0.0", port=8001)
#     asyncio.run(main(publisher))
