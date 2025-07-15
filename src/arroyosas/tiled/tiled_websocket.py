import asyncio
import json
import logging
import time
from functools import reduce
import operator as op

import websockets
import numpy as np
from arroyopy.listener import Listener
from arroyopy.operator import Operator
from tiled.client import from_uri
from tiled.client.container import Container

from ..schemas import (
    RawFrameEvent,
    SASStart,
    SASStop,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)

class TiledWebSocketListener(Listener):
    """
    WebSocket listener for Tiled events (Phase 1 implementation).
    This listener receives notifications about runs and frames via WebSocket,
    then fetches the actual data from Tiled.
    
    For Phase 2 (direct data transfer), subclass this and override the
    _handle_message and _handle_new_frame methods.
    """
    
    def __init__(
        self,
        operator: Operator,
        beamline_runs_tiled: Container,
        tiled_frame_segments: list,
        poll_pause_sec: int,
        websocket_url: str,
        single_run: str = None
    ):
        self.operator = operator
        self.beamline_runs_tiled = beamline_runs_tiled
        self.tiled_frame_segments = tiled_frame_segments
        self.poll_pause_sec = poll_pause_sec  # Not used, but kept for API compatibility
        self.websocket_url = websocket_url
        self.single_run = single_run
        self._running = False
    
    async def start(self):
        """Start the listener by calling _start method."""
        self._running = True
        await self._start()
    
    async def _start(self):
        """
        Main implementation of the listener.
        In WebSocket mode, it establishes a connection and processes messages.
        In single run mode, it processes just that run.
        """
        # Single run mode - similar to original TiledPollingFrameListener
        if self.single_run:
            await self._process_single_run()
            return
        
        # WebSocket mode - continuous listening
        # Connect to WebSocket and listen for events
        while self._running:
            try:
                logger.info(f"Connecting to Tiled WebSocket at {self.websocket_url}")
                async with websockets.connect(self.websocket_url) as websocket:
                    logger.info("Connected to Tiled WebSocket")
                    
                    # Listen for messages
                    async for message in websocket:
                        if not self._running:
                            break
                            
                        try:
                            data = json.loads(message)
                            await self._handle_message(data)
                                
                        except Exception as e:
                            logger.exception(f"Error processing message: {e}")
                            
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                
            if self._running:
                # Wait before reconnecting
                await asyncio.sleep(2)
    
    async def _process_single_run(self):
        """Process a single run in isolation."""
        try:
            current_run = self.beamline_runs_tiled[self.single_run]
            logger.info(
                f"Processing single run: {current_run.metadata['start']['scan_id']} {current_run.metadata['start']['uid']}"
            )
            
            # Get metadata and send start message
            data = current_run[tuple(self.tiled_frame_segments)]
            start_message = SASStart(
                width=data.shape[0],
                height=data.shape[1],
                data_type=data.dtype.name,
                tiled_url=current_run.uri,
                run_name=str(current_run.metadata['start'].get('scan_id')),
                run_id=current_run.metadata['start']['uid'],
            )
            await self.operator.process(start_message)
            
            # Process all frames
            frames_array = self._get_frames_array(current_run)
            
            frames_index = 1
            if frames_array.shape[1] == 1:
                frames_index = 0
            
            num_frames = frames_array.shape[frames_index]
            
            for frame_num in range(num_frames):
                if frames_index == 1:
                    array = frames_array[0, frame_num]
                else:
                    array = frames_array[frame_num, 0]
                
                image = SerializableNumpyArrayModel(array=array)
                frame_event = RawFrameEvent(
                    image=image,
                    frame_number=frame_num,
                    tiled_url=f"{current_run.uri}/primary/data/pil1M_image",
                )
                await self.operator.process(frame_event)
                await asyncio.sleep(1)  # Small delay between frames
            
            # Send stop message
            stop_message = SASStop(num_frames=num_frames)
            await self.operator.process(stop_message)
        except Exception as e:
            logger.exception(f"Error processing single run: {e}")
    
    def _get_frames_array(self, run):
        """Get the frames array from a run."""
        frames_container = run
        for segment in self.tiled_frame_segments:
            frames_container = frames_container[segment]
        return frames_container
    
    async def _handle_message(self, message):
        """
        Process a message received from the WebSocket.
        This method dispatches to specific handlers based on message type.
        
        Override this method in subclasses to handle additional message types.
        """
        msg_type = message.get('type')
        
        if msg_type == 'run_start':
            await self._handle_run_start(message)
        elif msg_type == 'run_stop':
            await self._handle_run_stop(message)
        elif msg_type == 'new_frame':
            await self._handle_new_frame(message)
        else:
            logger.debug(f"Ignoring message type: {msg_type}")
    
    async def _handle_run_start(self, message):
        """Handle a run_start message."""
        # Get run ID and fetch run from Tiled
        run_id = message.get('run_id')
        try:
            current_run = self.beamline_runs_tiled[run_id]
            self.current_run = current_run
            self.sent_frames = []
            
            # Get metadata and send start message
            data = current_run[tuple(self.tiled_frame_segments)]
            start_message = SASStart(
                width=data.shape[0],
                height=data.shape[1],
                data_type=data.dtype.name,
                tiled_url=current_run.uri,
                run_name=str(current_run.metadata['start'].get('scan_id', run_id)),
                run_id=current_run.metadata['start']['uid'],
            )
            await self.operator.process(start_message)
        except Exception as e:
            logger.exception(f"Error handling run start: {e}")
            self.current_run = None
            self.sent_frames = []
    
    async def _handle_run_stop(self, message):
        """Handle a run_stop message."""
        # Check if this is the current run
        run_id = message.get('run_id')
        if not hasattr(self, 'current_run') or not self.current_run or self.current_run.metadata['start']['uid'] != run_id:
            return
            
        # Send stop message
        stop_message = SASStop(num_frames=len(self.sent_frames))
        await self.operator.process(stop_message)
        
        # Reset state
        self.current_run = None
        self.sent_frames = []
    
    async def _handle_new_frame(self, message):
        """
        Handle a new_frame message (Phase 1 implementation).
        
        In Phase 1, this receives a notification about a new frame,
        then fetches the actual data from Tiled.
        
        Override this method in subclasses for Phase 2 to handle
        direct data transfer.
        """
        # Get frame info
        run_id = message.get('run_id')
        frame_number = message.get('frame_number', 0)
        
        # Check if this is the current run and if we've already processed this frame
        if not hasattr(self, 'current_run') or not self.current_run or self.current_run.metadata['start']['uid'] != run_id:
            return
            
        if hasattr(self, 'sent_frames') and frame_number in self.sent_frames:
            return
            
        try:
            # Get the frames array
            frames_array = self._get_frames_array(self.current_run)
            
            # Get the frame data
            frames_index = 1
            if frames_array.shape[1] == 1:
                frames_index = 0
                
            if frames_index == 1:
                array = frames_array[0, frame_number]
            else:
                array = frames_array[frame_number, 0]
            
            # Create and send RawFrameEvent
            image = SerializableNumpyArrayModel(array=array)
            frame_event = RawFrameEvent(
                image=image,
                frame_number=frame_number,
                tiled_url=f"{self.current_run.uri}/primary/data/pil1M_image",
            )
            
            await self.operator.process(frame_event)
            if not hasattr(self, 'sent_frames'):
                self.sent_frames = []
            self.sent_frames.append(frame_number)
        except Exception as e:
            logger.exception(f"Error handling new frame: {e}")
    
    async def stop(self):
        """Stop the listener."""
        self._running = False
    
    async def listen(self):
        """Listen for messages (compatibility method)."""
        # Not used, but kept for API compatibility
        pass
    
    @classmethod
    def from_settings(cls, settings, operator):
        """Create a TiledWebSocketListener from settings."""
        # Same implementation as TiledPollingFrameListener.from_settings
        tiled_runs_segments = settings.runs_segments
        poll_pause_sec = settings.poll_interval
        
        client = from_uri(
            settings.uri,
            api_key=settings.api_key,
        )
        
        run_container = client[tuple(tiled_runs_segments.to_list())]
        logger.info(f"#### Listening for runs at {run_container.uri}")
        logger.info(f"#### Frames segments: {settings.frames_segments}")
        
        single_run = None
        if settings.get("single_run"):
            single_run = settings.single_run
            logger.info(f"#### Single run mode: {single_run}")
        
        # Get WebSocket URL from settings or derive it
        websocket_url = settings.get("websocket_url")
        if not websocket_url:
            # Derive WebSocket URL from Tiled URL
            base_url = settings.uri
            if base_url.endswith('/'):
                base_url = base_url[:-1]
            websocket_url = base_url.replace('http://', 'ws://').replace('https://', 'wss://') + "/stream"
            logger.info(f"#### Using derived WebSocket URL: {websocket_url}")
        else:
            logger.info(f"#### Using WebSocket URL from settings: {websocket_url}")
        
        return cls(
            operator=operator,
            beamline_runs_tiled=run_container,
            tiled_frame_segments=settings.frames_segments,
            poll_pause_sec=poll_pause_sec,
            websocket_url=websocket_url,
            single_run=single_run,
        )