# How do I run this the first time? Install everything and then run it as a module from the project ROOT
# pip install -e .
# python -m arroyosas.directWebsocketSim
import asyncio
import json
import logging
from typing import Union
from glob import glob
import os

from PIL import Image
import msgpack
import numpy as np
import websockets
from arroyopy.publisher import Publisher
from .one_d_reduction.reduce import pixel_roi_horizontal_cut
#from .one_d_reduction.operator import OneDReductionOperator #the methods needed in this file are now instance, not static

#copied from one_d_reduction.operator.py, add as static method in future refactor 
def generate_masked_image(image, mask):
    masked_float = mask.astype(float)  
    masked_float[masked_float == True] = np.nan  #At true values set to NaN
    masked_float[masked_float == 0] = 1 #At false values set to 1
    masked_image = image * masked_float #Multiply to set masked values to NaN
    return masked_image

#copied from one_d_reduction.operator.py, add as static method in future refactor 
def load_static_mask_file():
    try:
        # assumed path from project root is masks/mask.npy 
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        mask_path = os.path.join(project_root, "masks", "mask.npy")
        mask = np.load(mask_path)
        logger.info(f"Mask loaded successfully from {mask_path}, shape: {mask.shape}")
        return mask

    except FileNotFoundError:
        logger.error(f"Mask file not found at: {mask_path}")
    except Exception as e:
        logger.error(f"Error loading mask file: {e}")
    return None


from .schemas import  RawFrameEvent, SASStart, SASStop, SerializableNumpyArrayModel

logger = logging.getLogger(__name__)

mask = load_static_mask_file()
logger.info(f"Mask shape: {mask.shape if mask is not None else 'None'}")
#TO DO: handle conditions where detector image is 'flipped' so y values should be from (top - input_height)
parameters_smi = {
    "masked_image": None,
    "beamcenter_x": 251,
    "beamcenter_y": 200,
    "incident_angle": 0.15,
    "sample_detector_dist": 100,
    "wavelength": 1.2398,
    "pix_size": 172,
    "cut_half_width": 1,
    "cut_pos_y": 90,
    "x_min": 0,
    "x_max": 500,
    "output_unit": "q"
}

linecut = {
    "cut_half_width": parameters_smi["cut_half_width"],
    "cut_pos_y": parameters_smi["cut_pos_y"],
    "x_min": parameters_smi["x_min"],
    "x_max": parameters_smi["x_max"],
}

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

    async def publish(self, message: RawFrameEvent) -> None:
        if self.connected_clients:  # Only send if there are clients connected
            asyncio.gather(
                *(self.publish_ws(client, message) for client in self.connected_clients)
            )

    async def publish_ws(
        self,
        #  client: websockets.client.ClientConnection,
        client,
        message: Union[RawFrameEvent | SASStart | SASStop],
    ) -> None:
        if isinstance(message, SASStop):
            logger.info(f"WS Sending Stop {message}")
            self.current_start_message = None
            await client.send(json.dumps(message.model_dump()))
            return

        if isinstance(message, SASStart):
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


def pack_images(message: RawFrameEvent) -> bytes:
    """
    Pack all the images into a single msgpack message
    """
    try:
        masked_image = generate_masked_image(message.image.array, mask)
        parameters_smi["masked_image"] = masked_image
        q_coordinates, reduction, _ = pixel_roi_horizontal_cut(**parameters_smi)
        plot_array = np.column_stack((q_coordinates, reduction))
        serializable_reduction = SerializableNumpyArrayModel(array=plot_array)
        return msgpack.packb(
            {
                "raw_frame": convert_to_uint8(message.image.array),
                "curve": serializable_reduction.array.tobytes(),
                "raw_frame_tiled_url": message.tiled_url,
                "curve_tiled_url": message.tiled_url,
                "width": message.image.array.shape[1],
                "height": message.image.array.shape[0],
                "linecut": linecut
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
        RawFrameEvent,
        SASStart,
        SASStop,
        SerializableNumpyArrayModel
    )



    await asyncio.sleep(2)
    for y in range(100):
        await publisher.publish(SASStart(
            run_name="test_run",
            run_id="12345",
            width=1043,
            height=981,
            data_type="float32",
            tiled_url="http://example.com/tiled"
        ))
       
        files = glob("/Users/seij/SMI_Experiments/feb/*.tiff")
        print(f"Total files found: {len(files)}")
        frame_num = 0
        for file in files:
            with Image.open(file) as img:
                img = img.convert("L")  #Force greyscale
                arr = np.array(img)  # 
                link ="http://127.0.0.1:8000/api/v1/array/full/feb/pil1M_image?slice=" + str(frame_num)

                event = RawFrameEvent(
                    image=SerializableNumpyArrayModel(array=arr),
                    frame_number=frame_num,
                    tiled_url=link,
                )
                print("event")
                await publisher.publish(event)
            await asyncio.sleep(5)
            frame_num = frame_num + 1
        await publisher.publish(SASStop(num_frames=num_frames))


async def main(publisher: OneDWSPublisher):
    await asyncio.gather(publisher.start(), test_client(publisher))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    publisher = OneDWSPublisher(host="0.0.0.0", port=8001)
    asyncio.run(main(publisher))
