import json
import logging
import numpy as np

from ..schemas import RawFrameEvent, SerializableNumpyArrayModel
from .tiled_websocket import TiledWebSocketListener

logger = logging.getLogger(__name__)

class TiledDirectDataWebSocketListener(TiledWebSocketListener):
    """
    Phase 2 implementation of the WebSocket listener.
    This subclass handles direct data transfer over the WebSocket.
    """
    
    # No need to override __init__ since we're just inheriting it
    # The constructor already accepts websocket_url from the updated base class
    
    async def _handle_message(self, message):
        """
        Process a message received from the WebSocket.
        
        This override adds support for direct data transfer messages.
        """
        msg_type = message.get('type')
        
        # Handle frame_data messages (Phase 2)
        if msg_type == 'frame_data':
            await self._handle_frame_data(message)
        else:
            # Fall back to Phase 1 handling for other message types
            await super()._handle_message(message)
    
    async def _handle_frame_data(self, message):
        """
        Handle a frame_data message (Phase 2).
        
        This message contains the actual frame data, so we don't need
        to fetch it from Tiled.
        """
        # Get frame info
        run_id = message.get('run_id')
        frame_number = message.get('frame_number', 0)
        
        # Check if this is the current run and if we've already processed this frame
        if not hasattr(self, 'current_run') or not self.current_run or self.current_run.metadata['start']['uid'] != run_id:
            return
            
        if hasattr(self, 'sent_frames') and frame_number in self.sent_frames:
            return
        
        # Extract the frame data from the message
        data = message.get('data')
        if not data:
            logger.error("Frame data message missing 'data' field")
            return
        
        try:
            # Convert the data to a numpy array
            # The exact conversion will depend on the format Tiled uses
            array = self._convert_data_to_array(data, message)
            
            # Create and send RawFrameEvent
            image = SerializableNumpyArrayModel(array=array)
            frame_event = RawFrameEvent(
                image=image,
                frame_number=frame_number,
                tiled_url=f"{self.current_run.uri}/primary/data/pil1M_image",  # Include URL for reference
            )
            
            await self.operator.process(frame_event)
            if not hasattr(self, 'sent_frames'):
                self.sent_frames = []
            self.sent_frames.append(frame_number)
        except Exception as e:
            logger.exception(f"Error handling frame data: {e}")
    
    def _convert_data_to_array(self, data, message):
        """
        Convert the data received in a frame_data message to a numpy array.
        
        The exact implementation will depend on how Tiled sends the data.
        This is a placeholder that can be updated when the format is known.
        """
        # If data is a base64-encoded string
        if isinstance(data, str):
            import base64
            import numpy as np
            
            # Get dimensions from message or current run
            width = message.get('width')
            height = message.get('height')
            dtype = message.get('data_type', 'float32')
            
            if not width or not height:
                # Get dimensions from current run if not in message
                if hasattr(self, 'current_run'):
                    frames_array = self._get_frames_array(self.current_run)
                    width = frames_array.shape[1]
                    height = frames_array.shape[0]
            
            # Decode and reshape
            decoded = base64.b64decode(data)
            return np.frombuffer(decoded, dtype=dtype).reshape((height, width))
        
        # If data is a list of lists (2D array)
        elif isinstance(data, list) and all(isinstance(item, list) for item in data):
            return np.array(data)
        
        # If data is a flat list
        elif isinstance(data, list):
            # Get dimensions from message or current run
            width = message.get('width')
            height = message.get('height')
            
            if not width or not height:
                # Get dimensions from current run if not in message
                if hasattr(self, 'current_run'):
                    frames_array = self._get_frames_array(self.current_run)
                    width = frames_array.shape[1]
                    height = frames_array.shape[0]
            
            return np.array(data).reshape((height, width))
        
        # Default: return as is (if it's already a numpy array)
        return data