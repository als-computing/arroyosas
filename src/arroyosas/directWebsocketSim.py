#pip install -e .
#cd src/
import asyncio
import json
import logging
from typing import Union
from glob import glob

from PIL import Image
import msgpack
import numpy as np
import websockets
from arroyopy.publisher import Publisher

from .schemas import GISAXSRawEvent, GISAXSStart, GISAXSStop, SerializableNumpyArrayModel

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

    async def publish(self, message: GISAXSRawEvent) -> None:
        if self.connected_clients:  # Only send if there are clients connected
            asyncio.gather(
                *(self.publish_ws(client, message) for client in self.connected_clients)
            )

    async def publish_ws(
        self,
        #  client: websockets.client.ClientConnection,
        client,
        message: Union[GISAXSRawEvent | GISAXSStart | GISAXSStop],
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
    """
    Convert an image to uint8, scaling image
    """
    # scaled = (image - image.min()) / (image.max() - image.min()) * 255
    # return scaled.astype(np.uint8).tobytes()

    image_normalized = (image - image.min()) / (image.max() - image.min())

    # Apply logarithmic stretch
    log_stretched = np.log1p(image_normalized)  # log(1 + x) to handle near-zero values

    # Normalize the log-stretched image to [0, 1] again
    log_stretched_normalized = (log_stretched - log_stretched.min()) / (
        log_stretched.max() - log_stretched.min()
    )

    # Convert to uint8 (range [0, 255])
    image_uint8 = (log_stretched_normalized * 255).astype(np.uint8)

    return image_uint8.tobytes()


def pack_images(message: GISAXSRawEvent) -> bytes:
    """
    Pack all the images into a single msgpack message
    """
    try:
        return msgpack.packb(
            {
                "raw_frame": convert_to_uint8(message.image.array),
                #"curve": message.curve.df.to_json(),
                "raw_frame_tiled_url": message.tiled_url,
                "curve_tiled_url": message.tiled_url,
                "width": message.image.array.shape[1],
                "height": message.image.array.shape[0],
                #"data_type": message.image.array.dtype,
            }
        )
    except Exception as e:
        logger.error(f"Error packing images: {e}")
        raise e


async def test_client(publisher: OneDWSPublisher, num_frames: int = 10):
    import time

    import pandas as pd
    from arroyopy.schemas import DataFrameModel, NumpyArrayModel

    from arroyosas.schemas import (
       # GISAXSImageInfo,
        GISAXSRawEvent,
        GISAXSStart,
        GISAXSStop,
        SerializableNumpyArrayModel
    )

    await asyncio.sleep(2)
    for y in range(100):
        await publisher.publish(GISAXSStart(
            run_name="test_run",
            run_id="12345",
            width=1043,
            height=981,
            data_type="float32",
            tiled_url="http://example.com/tiled"
        ))
        # for x in range(num_frames):
        #     await asyncio.sleep(1)
        #     # Create a test pattern image that changes slightly each time
        #     frame_number = int(time.time()) % 100  # Change pattern every second


        #files = glob("/Users/seij/SMI_Experiments/exp01/*.tif")
        files = glob("/Users/seij/SMI_Experiments/feb/*.tiff")
        #path = "/Users/seij/SMI_Experiments/feb/"
        #for f in os.listdir(path):
         #   print(f"{repr(f)}")

        print(f"Total files found: {len(files)}")
        frame_num = 0
        for file in files:
            with Image.open(file) as img:
                #print(f"Image mode: {img.mode}")  # 'L', 'RGB', 'RGBA', or multi-band
                img = img.convert("L")  #Force greyscale
                arr = np.array(img)  # 
                #arr_normalized = (arr - arr.min()) / (arr.max() - arr.min()) * 255
                #arr_normalized = arr_normalized.astype(np.uint8)  # Convert to uint8
                #print(f"Loaded image {file}, shape: {arr.shape}, dtype: {arr.dtype}")
                #link ="http://127.0.0.1:8000/api/v1/array/full/exp01/ML_exp01-144J-22_id836920_?slice=" + str(frame_num) + ",::1,::1"
                link ="http://127.0.0.1:8000/api/v1/array/full/feb/pil1M_image?slice=" + str(frame_num)


                event = GISAXSRawEvent(
                    image=SerializableNumpyArrayModel(array=arr),
                    frame_number=frame_num,
                    tiled_url=link,
                )
                print("event")
                await publisher.publish(event)
            await asyncio.sleep(5)

            # #Creates a sample image 
            # image = np.zeros((100, 100), dtype=np.float32)
            # np.fill_diagonal(image, frame_number % 255)

            # # Create a 1D sine wave pattern
            # x = np.linspace(0, 2 * np.pi, 100)
            # one_d_reduction = pd.DataFrame(
            #     {"q": x, "qy": np.sin(x + frame_number * 0.1)}
            # )
            # image_info = {
            #     "frame_number": frame_number,
            #     "width": image.shape[1],
            #     "height": image.shape[0],
            #     "data_type": "uint8",
            # }

            # # Create GISAXSResult message
            # message = GISAXSRawEvent(
            #     #image_info=GISAXSImageInfo(**image_info),
            #     image=SerializableNumpyArrayModel(array=image),
            #     one_d_reduction=DataFrameModel(df=one_d_reduction),
            #     frame_number=frame_number,
            #     tiled_url="fake_url"
            # )

            #await publisher.publish(message)
            frame_num = frame_num + 1
        await publisher.publish(GISAXSStop(num_frames=num_frames))


async def main(publisher: OneDWSPublisher):
    await asyncio.gather(publisher.start(), test_client(publisher))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    publisher = OneDWSPublisher(host="0.0.0.0", port=8001)
    asyncio.run(main(publisher))
