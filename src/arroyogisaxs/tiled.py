import asyncio
import logging
import operator
from functools import reduce
import time
from typing import Union

import numpy as np
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from arroyopy.publisher import Publisher
from tiled.client import from_uri
from tiled.client.array import ArrayClient
from tiled.client.base import BaseClient

# from tiled.client.dataframe import DataFrameClient
from tiled.client.container import Container

from .schemas import (
    GISAXS1DReduction,
    GISAXSLatentSpaceEvent,
    GISAXSMessage,
    GISAXSRawEvent,
    GISAXSStart,
    GISAXSStop,
    SerializableNumpyArrayModel,
)

RUNS_CONTAINER_NAME = "runs"

logger = logging.getLogger(__name__)


class TiledTestframeListener(Listener):
    def __init__(self, tiled_array: ArrayClient):
        super().__init__()
        self.tiled_array = tiled_array

    async def start(self):
        while True:
            # Get the most recent run
            for frame in self.tiled_array:
                print(frame)
            # # if cent_tiled_run = most_recent_run(self.beamline_runs_tiled)
            # start_message = GISAXSStart(
            #     width=current_tiled_run.width,
            #     height=current_tiled_run.height,
            #     data_type=current_tiled_run.data_type,
            # )
            # await self.operator.process(start_message)

            # # If run has stop document, send GISAXSStop message and
            # # set_current_run to None, sent_frames to [] and continue
            # if current_tiled_run.has_stop_document():
            #     stop_message = GISAXSStop(num_frames=len(sent_frames))
            #     await self.operator.process(stop_message)
            #     current_tiled_run = None
            #     sent_frames = []
            #     continue

            # # How many frames in this run?
            # # if len(sent_frames) == num_frames, continue
            # # if len(sent_frames) < num_frames, get the next N frames and
            # #   for each new frame
            # #        construct a GISAXSRawEvent and call operator.process()
            # #        add frame number to sent_frames
            # frames_array = sub_container(current_tiled_run, self.tiled_frame_segments)

            # if sent_frames == frames_array.shape[0]:
            #     # Sleep for poll_interval
            #     await asyncio.sleep(self.poll_paust_sec)
            #     continue
            # unsents = unsent_frame_numbers(sent_frames, frames_array.shape[0])
            # for unsent_frame in unsents:
            #     array = frames_array[unsent_frame]
            #     raw_event = GISAXSRawEvent(image=array, frame_number=len(sent_frames))
            #     await self.operator.process(raw_event)
            #     sent_frames.append(unsent_frame)

    async def stop(self):
        pass

    async def listen(self):
        pass

    @classmethod
    def from_uri(cls, tiled_uri: str, path: str):
        client = from_uri(tiled_uri)
        tiled_node = from_uri(client.uri + path)
        node = from_uri(tiled_node)
        return cls(node)


class TiledPollingFrameListener(Listener):
    def __init__(
        self,
        operator: Operator,
        beamline_runs_tiled: Container,
        tiled_frame_segments: list,
        poll_pause_sec: int,
        single_run: str = None
    ):
        self.beamline_runs_tiled = beamline_runs_tiled
        self.poll_pause_sec = poll_pause_sec
        self.tiled_frame_segments = tiled_frame_segments
        self.operator = operator
        self.single_run = single_run
    
    async def start(self):
        await asyncio.to_thread(self._start) 

    def _start(self):
        last_processed_run = None
        sent_frames = []
        while True:
            try:
                if self.single_run and last_processed_run:
                    logger.info("Single run mode, exiting")
                    break
                if self.single_run:
                    current_run = self.beamline_runs_tiled[self.single_run]
                else:
                    time.sleep(self.poll_pause_sec)
                    current_run = get_most_recent_run(self.beamline_runs_tiled)
                    logger.info(
                        f"Most Recent run: {current_run.metadata['start']['scan_id']} {current_run.metadata['start']['uid']}"
                    )
                    if last_processed_run and current_run.start["scan_id"] == last_processed_run.start["scan_id"]:
                        logger.debug("No new runs")
                        time.sleep(self.poll_pause_sec)
                        continue
                logger.info(
                    f"Processing: {current_run.metadata['start']['scan_id']} {current_run.metadata['start']['uid']}"
                )
                data = current_run[tuple(self.tiled_frame_segments.to_list())]
                start_message = GISAXSStart(
                    width=data.shape[0],
                    height=data.shape[1],
                    data_type=data.dtype.name,
                    tiled_url=current_run.uri,
                    run_name=str(current_run.metadata["start"]["scan_id"]),
                    run_id=current_run.metadata["start"]["uid"],
                )
                asyncio.run(self.operator.process(start_message))
                # How many frames in this run?
                # if len(sent_frames) == num_frames, continue
                # if len(sent_frames) < num_frames, get the next N frames and
                #   for each new frame
                #        construct a GISAXSRawEvent and call operator.process()
                #        add frame number to sent_frames
                frames_array = sub_container(current_run, self.tiled_frame_segments)
                logger.debug(f"Frames array shape: {frames_array.shape}")
                if len(sent_frames) == frames_array.shape[0]:
                    # Sleep for poll_interval
                    time.sleep(self.poll_pause_sec)
                    continue
                # the shape of these arrays changs from (1, n, x, y) to (n, 1, x, x)
                frames_index = 1
                if frames_array.shape[1] == 1:
                    frames_index = 0
                unsents = unsent_frame_numbers(
                    sent_frames, frames_array.shape[frames_index]
                )
                logger.debug(f"Unsent frames: {unsents}")
                for unsent_frame in unsents:
                    if frames_index == 1:
                        array = frames_array[0, unsent_frame]
                    else:
                        array = frames_array[unsent_frame, 0]
                    image = SerializableNumpyArrayModel(array=array)
                    raw_event = GISAXSRawEvent(
                        image=image,
                        frame_number=unsent_frame,
                        tiled_url=current_run.uri + "/primary/data/pil1M_image",
                    )
                    logger.debug(f"Sending frame {unsent_frame}")
                    asyncio.run(self.operator.process(raw_event))
                    sent_frames.append(unsent_frame)
                    time.sleep(1)
                    # If run has stop document, send GISAXSStop message and
                # set_current_run to None, sent_frames to [] and continue
                if current_run.metadata["stop"]:
                    stop_message = GISAXSStop(num_frames=len(sent_frames))
                    asyncio.run(self.operator.process(stop_message))
                    sent_frames = []
                    continue
                last_processed_run = current_run
            except Exception as e:
                logger.exception(f"Error in polling loop: {e}")

    async def stop(self):
        pass

    async def listen(self):
        pass

    @classmethod
    def from_settings(cls, settings: dict, operator: Operator):
        tiled_runs_segments = settings.runs_segments
        poll_pause_sec = settings.poll_interval
        client = from_uri(
            settings.uri,
            api_key=settings.api_key,
        )
        run_container = client[tuple(tiled_runs_segments.to_list())]
        logger.info(f"#### Listening for runs at {run_container.uri}")
        logger.info(f"#### Polling interval: {poll_pause_sec}")
        logger.info(f"#### Frames segments: {settings.frames_segments}")
        single_run = None
        if settings.get("single_run"):
            single_run = settings.single_run
        return cls(
            operator,
            run_container,
            tiled_frame_segments=settings.frames_segments,
            poll_pause_sec=poll_pause_sec,
            single_run=single_run,
        )


class TiledRawFrameOperator(Operator):
    def __init__(self):
        super().__init__()
  
    async def process(self, message: GISAXSMessage) -> GISAXSMessage:
        await self.publish(message)


def get_most_recent_run(tiled_runs: Container):
    uid = tiled_runs.keys()[-1]
    return tiled_runs[uid]


def sub_container(run: Container, segments: list):
    container = reduce(operator.getitem, segments, run)
    return container


def unsent_frame_numbers(sent_frames: list, num_frames: int):
    if len(sent_frames) == 0:
        return list(range(num_frames))
    # Find the gaps in my_list
    gaps = [
        i for i in range(min(sent_frames), max(sent_frames) + 1) if i not in sent_frames
    ]

    # Find numbers between new_number and the max of my_list
    extra_numbers = list(range(max(sent_frames) + 1, num_frames + 1))

    # Combine both lists
    return gaps + extra_numbers


class TiledProcessedPublisher(Publisher):
    run_node = None
    one_d_array_node = None
    dim_reduced_array_node = None

    def __init__(self, root_container: Container) -> None:
        super().__init__()
        self.root_container = root_container

    async def publish(self, message: Union[GISAXSStart | GISAXS1DReduction]) -> None:
        # run_client = get_nested_client(self.client, self.run_path)
        try:
            if isinstance(message, GISAXSStart):
                self.run_node = await asyncio.to_thread(
                    get_run_container, self.root_container, message
                )
                return
            if self.run_node is None:
                logger.error("No run node found. Probably started after start message.")
                return
            elif isinstance(message, GISAXSStop):
                return

            if isinstance(message, GISAXS1DReduction):
                if self.one_d_array_node is None:
                    one_d_array_node = await asyncio.to_thread(
                        create_one_d_node, self.run_node, message
                    )
                    self.one_d_array_node = one_d_array_node
                else:
                    await asyncio.to_thread(self.update_1d_nodes, message)

            if isinstance(message, GISAXSLatentSpaceEvent):
                if self.dim_reduced_array_node is None:
                    dim_reduced_array_node = await asyncio.to_thread(
                        create_dim_reduction_node, self.run_node, message
                    )
                    self.dim_reduced_array_node = dim_reduced_array_node
                else:
                    print("there")
                    # print(self.dim_reduced_array_node)
                    await asyncio.to_thread(self.update_ls_nodes, message)
        except Exception as e:
            logger.error(f"Error in publisher: {e}")  

    def update_1d_nodes(self, message: GISAXS1DReduction) -> None:
        patch_tiled_frame(self.one_d_array_node, message.curve.array)

    def update_ls_nodes(self, message: GISAXSLatentSpaceEvent) -> None:
        patch_tiled_frame(self.dim_reduced_array_node, np.array(message.feature_vector))

    def get_run_path(self, message):
        return message.run_id

    @classmethod
    def from_settings(cls, settings: dict):
        client = from_uri(settings.uri, api_key=settings.api_key)
        segments = settings.root_segments
        root_container = get_runs_container(client, segments)
        return cls(root_container)


def create_one_d_node(run_node: Container, message: GISAXS1DReduction) -> None:
    one_d_array_node = run_node.write_array(
        message.curve.array[np.newaxis, :], key="one_d_reduction"
    )
    return one_d_array_node


def create_dim_reduction_node(run_node: Container, message: GISAXS1DReduction) -> None:
    arr = np.array(message.feature_vector)
    dim_reduction_node = run_node.write_array(arr[np.newaxis, :], key="dim_reduction")
    return dim_reduction_node


def get_runs_container(client: Container, root_segments: list) -> Container:
    seg_tuple = tuple(root_segments.to_list())
    viz_processed_container = client[seg_tuple]
    if RUNS_CONTAINER_NAME not in viz_processed_container:
        return viz_processed_container.create_container(RUNS_CONTAINER_NAME)
    return viz_processed_container[RUNS_CONTAINER_NAME]


def get_run_container(
    runs_container: Container, start_message: GISAXSStart
) -> Container:
    run_name = start_message.run_name + "_" + start_message.run_id
    if run_name not in runs_container:
        return runs_container.create_container(run_name)
    return runs_container[run_name]


# def tiled_run_uri_from_start_message(root_container: Container, message: GISAXSStart):
#     uri = f"{root_container.uri}/{message.run_name}_{message.run_id}"
#     return uri

# def one_d_reduction_uri(run_container: Container, message: GISAXSStart):
#     run_container_uri = tiled_run_uri_from_start_message(run_container, message)
#     uri = f"{run_container_uri}/one_d_reduction"
#     return uri

# def dim_reduction_uri(run_container_uri: str):
#     uri = f"{run_container_uri}/dim_reduction"
#     return uri

# def latent_space_uri(run_container_uri: str):
#     uri = f"{run_container_uri}/latent_space"
#     return uri


def create_run_container(client: Container, name: str) -> Container:
    if name not in client:
        return client.create_container(name)
    return client[name]


def get_nested_client(client: BaseClient, path) -> BaseClient:
    # Wow this is slow!
    client = from_uri(client.uri + path, api_key=client.context.api_key)
    return client


def create_array_node(
    run_container: Container, key: str, array: np.ndarray
) -> ArrayClient:
    return run_container.write_array(array, key=key)


def patch_tiled_frame(array_client: ArrayClient, array: np.ndarray) -> None:
    shape = array_client.shape
    offset = (shape[0],)
    array_client.patch(array[None, :], offset=offset, extend=True)
