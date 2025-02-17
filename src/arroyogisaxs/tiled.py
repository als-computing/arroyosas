import asyncio
import operator
from functools import reduce

from arroyopy.listener import Listener
from arroyopy.operator import Operator
from arroyopy.publisher import Publisher
from tiled.client import from_uri
from tiled.client.base import BaseClient
from tiled.client.container import Container

from .schemas import GISAXS1DReduction, GISAXSRawEvent, GISAXSStart, GISAXSStop


class Tiled1DPublisher(Publisher):
    async def publish(self, message: GISAXS1DReduction) -> None:
        pass


class TiledPollingFrameListener(Listener):
    def __init__(
        self,
        operator: Operator,
        beamline_runs_tiled: Container,
        tiled_frame_segments: list,
        poll_paust_sec: int,
    ):
        self.beamline_runs_tiled = beamline_runs_tiled
        self.poll_paust_sec = poll_paust_sec
        self.tiled_frame_segments = tiled_frame_segments
        self.operator = operator

    async def start(self):
        current_tiled_run = None
        sent_frames = []
        # loop = asyncio.get_event_loop()
        while True:
            # Get the most recent run

            # if current_tiled_run is None, get the most recent run, set it to
            # current and send GISAXSStart message
            if current_tiled_run is None:
                current_tiled_run = most_recent_run(self.beamline_runs_tiled)
                start_message = GISAXSStart(
                    width=current_tiled_run.width,
                    height=current_tiled_run.height,
                    data_type=current_tiled_run.data_type,
                )
                await self.operator.process(start_message)

            # If run has stop document, send GISAXSStop message and
            # set_current_run to None, sent_frames to [] and continue
            if current_tiled_run.has_stop_document():
                stop_message = GISAXSStop(num_frames=len(sent_frames))
                await self.operator.process(stop_message)
                current_tiled_run = None
                sent_frames = []
                continue

            # How many frames in this run?
            # if len(sent_frames) == num_frames, continue
            # if len(sent_frames) < num_frames, get the next N frames and
            #   for each new frame
            #        construct a GISAXSRawEvent and call operator.process()
            #        add frame number to sent_frames
            frames_array = sub_container(current_tiled_run, self.tiled_frame_segments)

            if sent_frames == frames_array.shape[0]:
                # Sleep for poll_interval
                await asyncio.sleep(self.poll_paust_sec)
                continue
            unsents = unsent_frame_numbers(sent_frames, frames_array.shape[0])
            for unsent_frame in unsents:
                array = frames_array[unsent_frame]
                raw_event = GISAXSRawEvent(image=array, frame_number=len(sent_frames))
                await self.operator.process(raw_event)
                sent_frames.append(unsent_frame)

    async def stop(self):
        pass

    async def listen(self):
        pass


class TiledRawFrameOperator(Operator):
    async def process(self, message: GISAXS1DReduction) -> GISAXS1DReduction:
        pass


def most_recent_run(tiled_runs: Container):
    uid = tiled_runs.keys()[-1]
    return tiled_runs[uid]


def sub_container(run: Container, segments: list):
    container = reduce(operator.getitem, segments, run)
    return container


def unsent_frame_numbers(sent_frames: list, num_frames: int):
    # Find the gaps in my_list
    gaps = [
        i for i in range(min(sent_frames), max(sent_frames) + 1) if i not in sent_frames
    ]

    # Find numbers between new_number and the max of my_list
    extra_numbers = list(range(max(sent_frames) + 1, num_frames + 1))

    # Combine both lists
    return gaps + extra_numbers


def get_nested_client(client: BaseClient, path) -> BaseClient:
    # Wow this is slow!
    client = from_uri(client.uri + path, api_key=client.context.api_key)
    return client
