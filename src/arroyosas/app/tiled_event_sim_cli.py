import asyncio
import json
import logging
from pathlib import Path
import re

import typer
import websockets

from ..config import settings
from ..log_utils import setup_logger

app = typer.Typer()
logger = logging.getLogger("arroyosas")
setup_logger(logger)

class TiledEventSimulator:
    """
    Simulator that reads Tiled event logs and streams them via WebSocket
    to test listeners without needing a real Tiled server.
    """
    def __init__(self, log_dir, host="0.0.0.0", port=8000, stream_path="/stream", run_id=None):
        self.log_dir = Path(log_dir)
        self.host = host
        self.port = port
        self.stream_path = stream_path
        self.specified_run_id = run_id
        self.connected_clients = set()
        
    async def start(self):
        """Start the WebSocket server."""
        server = await websockets.serve(
            self.handle_client, self.host, self.port
        )
        logger.info(f"Tiled Event Simulator running at ws://{self.host}:{self.port}{self.stream_path}")
        
        # List available runs at startup
        runs = self.list_available_runs()
        if runs:
            logger.info(f"Available runs: {', '.join(runs)}")
            
            if self.specified_run_id:
                if self.specified_run_id in runs:
                    logger.info(f"Will replay specified run: {self.specified_run_id}")
                else:
                    logger.warning(f"Specified run '{self.specified_run_id}' not found. Available runs: {', '.join(runs)}")
                    logger.info(f"Will use first available run: {runs[0]}")
            else:
                logger.info(f"No run specified. Will use first available run: {runs[0]}")
        else:
            logger.error(f"No run directories found in {self.log_dir}")
        
        await server.wait_closed()
    
    def list_available_runs(self):
        """List all available run directories."""
        return [d.name for d in self.log_dir.iterdir() 
                if d.is_dir() and any(d.glob("*.json"))]
        
    async def handle_client(self, websocket, path):
        """Handle a client connection."""
        # Only respond to connections on the stream path
        if path != self.stream_path:
            logger.warning(f"Client attempted to connect on incorrect path: {path}")
            return
        
        logger.info(f"New client connected from {websocket.remote_address}")
        self.connected_clients.add(websocket)
        
        try:
            # Get available runs
            runs = self.list_available_runs()
            if not runs:
                logger.error(f"No run directories found in {self.log_dir}")
                await websocket.send(json.dumps({
                    "type": "error",
                    "message": "No run directories found"
                }))
                return
                
            # Determine which run to replay
            run_id = self.specified_run_id if self.specified_run_id in runs else runs[0]
            logger.info(f"Replaying run: {run_id}")
            
            # Replay the selected run
            await self.replay_run(websocket, run_id)
            
            # Keep connection open
            try:
                await websocket.wait_closed()
            except websockets.exceptions.ConnectionClosed:
                pass
                
        finally:
            self.connected_clients.remove(websocket)
            logger.info("Client disconnected")
    
    async def replay_run(self, websocket, run_id):
        """Replay a run by sending events in sequence."""
        run_dir = self.log_dir / run_id
        
        if not run_dir.exists() or not run_dir.is_dir():
            logger.error(f"Run directory not found: {run_dir}")
            return
        
        # Collect all event files
        event_files = list(run_dir.glob("*.json"))
        if not event_files:
            logger.error(f"No event files found in {run_dir}")
            return
            
        # Parse event files to extract metadata
        events = []
        for file_path in event_files:
            try:
                with open(file_path) as f:
                    event_data = json.load(f)
                    
                # Extract important fields for sorting
                event_type = event_data.get("event_name")
                sequence = event_data.get("sequence", 0)
                timestamp = event_data.get("timestamp", 0)
                
                # For on_event, also get sequence from callback_data
                callback_sequence = None
                if event_type == "on_event" and "callback_data" in event_data:
                    callback_sequence = event_data["callback_data"].get("sequence")
                
                events.append({
                    "file_path": file_path,
                    "event_type": event_type,
                    "sequence": sequence,
                    "callback_sequence": callback_sequence,
                    "timestamp": timestamp,
                    "data": event_data
                })
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error reading {file_path}: {e}")
        
        # Group events by type for logical ordering
        events_by_type = {
            "on_new_run": [],
            "on_streams_namespace": [],
            "on_new_stream": [],
            "on_node_in_stream": [],
            "on_event": []
        }
        
        for event in events:
            event_type = event["event_type"]
            if event_type in events_by_type:
                events_by_type[event_type].append(event)
        
        # Sort each group by sequence number
        for event_type in events_by_type:
            # For on_event, sort by callback sequence which is the frame number
            if event_type == "on_event":
                events_by_type[event_type].sort(key=lambda e: e["callback_sequence"] if e["callback_sequence"] is not None else e["sequence"])
            else:
                events_by_type[event_type].sort(key=lambda e: e["sequence"])
        
        # Define the logical order of event types to send
        logical_order = [
            "on_new_run",           # First, the run is created
            "on_streams_namespace", # Then, the streams namespace appears
            "on_new_stream",        # Then individual streams are created
            "on_node_in_stream",    # Then nodes appear in those streams
            "on_event"              # Finally, events with actual data arrive
        ]
        
        # Log the event counts
        for event_type in logical_order:
            count = len(events_by_type[event_type])
            if count > 0:
                logger.info(f"Found {count} {event_type} events in run {run_id}")
            
        # Send events in logical order
        for event_type in logical_order:
            if not events_by_type[event_type]:
                continue
                
            logger.info(f"Sending {event_type} events...")
            for event in events_by_type[event_type]:
                # Extract the callback data which is what Tiled would send
                callback_data = event["data"].get("callback_data", {})
                
                try:
                    # Send the event data
                    await websocket.send(json.dumps(callback_data))
                    
                    if event_type == "on_event":
                        seq_info = f"(frame {event['callback_sequence']})"
                    else:
                        seq_info = f"(sequence {event['sequence']})"
                        
                    logger.info(f"Sent {event_type} event {seq_info}")
                    
                    # Add a small delay to simulate realistic timing
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error sending event: {e}")
                    return
        
        logger.info(f"Finished replaying run {run_id}")

@app.command()
def main(
    log_dir: str = typer.Argument(..., help="Directory containing Tiled event logs"),
    host: str = typer.Option("0.0.0.0", help="Host to bind the WebSocket server to"),
    port: int = typer.Option(8000, help="Port to bind the WebSocket server to"),
    stream_path: str = typer.Option("/", help="WebSocket path for the stream endpoint"),
    run_id: str = typer.Option(None, help="Specific run ID to replay (defaults to first run found)")
):
    """
    Start a Tiled event simulator that replays recorded events over WebSocket.
    This allows testing of listeners without a real Tiled server.
    
    To test with your listener:
    1. Start this simulator: python -m arroyosas.app.tiled_event_sim_cli --log-dir ./tiled_event_logs
    2. In another terminal, start your listener: python -m arroyosas.app.tiled_ws_cli --websocket-url ws://localhost:8000/stream
    
    The simulator will replay the events and your listener should process them normally.
    """
    async def run():
        simulator = TiledEventSimulator(log_dir, host, port, stream_path, run_id)
        await simulator.start()

    logger.info(f"Starting Tiled Event Simulator with logs from {log_dir}")
    logger.info(f"Clients can connect to ws://{host}:{port}{stream_path}")
    asyncio.run(run())

if __name__ == "__main__":
    app()