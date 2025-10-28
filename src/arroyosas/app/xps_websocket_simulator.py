"""
XPS WebSocket Simulator for arroyosas package

Loads XPS data from combined_all_bin_averages.npy and streams via WebSocket
in the format expected by the XPS processing system (tr_ap_xps format).

Data structure: Dictionary with keys (bin_num, shot_num) -> 2D numpy array
Example: combined_averages[(5, 70)] = heatmap array

Usage:
    python -m arroyosas.app.xps_websocket_simulator --data-file /data/xps_test/combined_all_bin_averages.npy
"""

import asyncio
import json
import logging
import os
import uuid
from pathlib import Path
from typing import List, Tuple, Dict

import msgpack
import numpy as np
import typer
import websockets

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: (%(name)s) %(message)s'
)

app = typer.Typer()


def load_xps_data(data_file: str) -> Tuple[np.ndarray, Dict[int, Tuple[int, int]]]:
    """
    Load XPS data from combined_all_bin_averages.npy
    
    Args:
        data_file: Path to combined_all_bin_averages.npy
        
    Returns:
        Tuple of:
        - 3D numpy array (total_entries, height, width)
        - Index map: {index: (bin_num, shot_num)}
    """
    try:
        # Load the combined dictionary
        combined_averages = np.load(data_file, allow_pickle=True).item()
        
        if not isinstance(combined_averages, dict):
            raise ValueError(f"Expected dict, got {type(combined_averages)}")
        
        # Get all keys, sorted by bin then shot number
        all_keys = sorted(combined_averages.keys())
        total_entries = len(all_keys)
        
        if total_entries == 0:
            raise ValueError("No data found in file")
        
        # Get shape from first entry
        first_key = all_keys[0]
        single_image_shape = combined_averages[first_key].shape
        
        logger.info(f"Data structure:")
        logger.info(f"  Total entries: {total_entries}")
        logger.info(f"  Image shape: {single_image_shape}")
        
        # Create 3D array
        all_data = np.zeros((total_entries, *single_image_shape), dtype=np.float32)
        
        # Create index map
        index_map = {}
        
        # Fill array and map
        for i, key in enumerate(all_keys):
            all_data[i] = combined_averages[key]
            index_map[i] = key  # key is (bin_num, shot_num)
        
        # Log bin distribution
        bins = set(bin_num for bin_num, _ in all_keys)
        logger.info(f"  Number of bins: {len(bins)}")
        logger.info(f"  Bin numbers: {sorted(bins)}")
        
        # Count shots per bin
        from collections import defaultdict
        shots_per_bin = defaultdict(int)
        for bin_num, _ in all_keys:
            shots_per_bin[bin_num] += 1
        
        for bin_num in sorted(shots_per_bin.keys()):
            logger.info(f"  Bin {bin_num}: {shots_per_bin[bin_num]} shots")
        
        return all_data, index_map
        
    except Exception as e:
        logger.error(f"Error loading data file: {e}")
        raise


def prepare_start_message() -> str:
    """
    Prepare XPS start message in JSON format.
    Matches the tr_ap_xps XPSStart schema.
    """
    scan_uuid = str(uuid.uuid4())
    start_message = {
        "msg_type": "start",
        "scan_name": f"temp name {scan_uuid}",
        "F_Trigger": 13,
        "F_Un-Trigger": 38,
        "F_Dead": 45,
        "F_Reset": 46,
        "CCD_nx": 1392,
        "CCD_ny": 1040,
        "Pass Energy": 200,
        "Center Energy": 3308,
        "Offset Energy": -0.837,
        "Lens Mode": "X6-26Mar2022-test",
        "Rectangle": {
            "Left": 148,
            "Top": 385,
            "Right": 1279,
            "Bottom": 654,
            "Rotation": 0
        },
        "data_type": "U8",
        "dt": 0.0820741786426572,
        "Photon Energy": 3999.99740398402,
        "Binding Energy": 90,
        "File Ver": "1.0.0"
    }
    return json.dumps(start_message)


def prepare_stop_message() -> str:
    """
    Prepare XPS stop message in JSON format.
    Matches the tr_ap_xps XPSStop schema.
    """
    stop_message = {
        "msg_type": "stop",
        "Num Frames": 0
    }
    return json.dumps(stop_message)


def convert_to_uint8(image: np.ndarray) -> bytes:
    """
    Convert an image to uint8 with logarithmic scaling.
    Matches the tr_ap_xps/websockets.py convert_to_uint8 function.
    """
    if image.size == 0:
        return b''
    
    # Normalize to [0, 1]
    image_normalized = (image - image.min()) / (image.max() - image.min() + 1e-10)
    
    # Apply logarithmic stretch
    log_stretched = np.log1p(image_normalized)
    
    # Normalize again
    log_stretched_normalized = (log_stretched - log_stretched.min()) / (
        log_stretched.max() - log_stretched.min() + 1e-10
    )
    
    # Convert to uint8
    image_uint8 = (log_stretched_normalized * 255).astype(np.uint8)
    return image_uint8.tobytes()


def prepare_xps_message(
    shot_num: int, 
    bin_num: int,
    shot_mean: np.ndarray
) -> bytes:
    """
    Prepare XPS message matching the tr_ap_xps format.
    
    Args:
        shot_num: Shot number within current bin
        bin_num: Current bin number
        shot_mean: 2D numpy array with heatmap data
        
    Returns: msgpack binary message
    """
    # Ensure 2D shape
    if shot_mean.ndim != 2:
        logger.warning(f"Expected 2D array, got shape {shot_mean.shape}")
        if shot_mean.ndim == 1:
            shot_mean = shot_mean.reshape(1, -1)
        else:
            shot_mean = shot_mean.reshape(shot_mean.shape[0], -1)
    
    height, width = shot_mean.shape
    
    # msgpack with image data (tr_ap_xps format)
    msgpack_data = {
        "shot_num": shot_num,
        "shot_mean": convert_to_uint8(shot_mean),
        "shot_recent": convert_to_uint8(shot_mean),
        "shot_std": convert_to_uint8(np.zeros_like(shot_mean)),
        "width": height,  # Note: swapped in real system
        "height": width,
        "raw": convert_to_uint8(shot_mean),
        "vfft": convert_to_uint8(shot_mean),
        "ifft": convert_to_uint8(shot_mean),
        "fitted": json.dumps([])
    }
    
    return msgpack.packb(msgpack_data)


async def send_xps_data(
    websocket, 
    shot_num: int, 
    bin_num: int,
    shot_mean: np.ndarray
):
    """Send XPS data through WebSocket"""
    try:
        msgpack_msg = prepare_xps_message(shot_num, bin_num, shot_mean)
        msg_size = len(msgpack_msg)
        
        await websocket.send(msgpack_msg)
        logger.info(f"✓ Sent bin {bin_num:2d}, shot {shot_num:2d} | shape={shot_mean.shape} | size={msg_size:,} bytes")
        
    except Exception as e:
        logger.error(f"✗ Error sending bin {bin_num}, shot {shot_num}: {e}")


async def websocket_handler(
    websocket,
    all_data: np.ndarray,
    index_map: Dict[int, Tuple[int, int]],
    cycles: int,
    pause: float,
    bins_to_send: List[int] = None
):
    """
    Handle WebSocket connection and send XPS data.
    
    Args:
        websocket: WebSocket connection
        all_data: 3D array (entries, height, width)
        index_map: Maps index -> (bin_num, shot_num)
        cycles: Number of times to repeat the data
        pause: Pause between shots in seconds
        bins_to_send: List of bin numbers to send (None = all bins)
    """
    client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    logger.info("=" * 70)
    logger.info(f"📌 New WebSocket connection from {client_info}")
    logger.info(f"📝 Path: {websocket.request.path}")
    logger.info("=" * 70)
    
    # Check path
    if websocket.request.path != "/simImages":
        logger.warning(f"⚠️  Unexpected path: {websocket.request.path}, expected /simImages")
    
    try:
        for cycle in range(cycles):
            logger.info("")
            logger.info("=" * 70)
            logger.info(f"🔄 Cycle {cycle + 1}/{cycles}")
            logger.info("=" * 70)
            
            # Group indices by bin
            from collections import defaultdict
            bin_indices = defaultdict(list)
            for idx, (bin_num, shot_num) in index_map.items():
                if bins_to_send is None or bin_num in bins_to_send:
                    bin_indices[bin_num].append((idx, shot_num))
            
            # Send data bin by bin - each bin gets its own START/STOP
            for bin_num in sorted(bin_indices.keys()):
                logger.info(f"")
                logger.info(f"📦 Starting bin {bin_num}")
                
                # Send start message for this bin
                start_msg = prepare_start_message()
                # Extract UUID from the start message for logging
                start_data = json.loads(start_msg)
                scan_uuid = start_data['scan_name'].replace('temp name ', '').strip()
                await websocket.send(start_msg)
                logger.info(f"📤 Sent START message for bin {bin_num} with UUID: {scan_uuid}")
                await asyncio.sleep(0.1)
                
                logger.info(f"   Total shots in bin: {len(bin_indices[bin_num])}")
                
                # Sort shots within bin
                shots = sorted(bin_indices[bin_num], key=lambda x: x[1])
                
                # Track timing
                import time
                bin_start = time.time()
                
                # Send all shots for this bin
                for idx, original_shot_num in shots:
                    shot_num = original_shot_num
                    shot_mean = all_data[idx]
                    
                    await send_xps_data(websocket, shot_num, bin_num, shot_mean)
                    await asyncio.sleep(pause)
                
                # Send stop message for this bin
                stop_msg = prepare_stop_message()
                await websocket.send(stop_msg)
                logger.info(f"📤 Sent STOP message for bin {bin_num}")
                
                bin_duration = time.time() - bin_start
                logger.info(f"✅ Completed bin {bin_num} | {len(shots)} shots in {bin_duration:.2f}s")
            
            if cycle < cycles - 1:
                logger.info("")
                logger.info(f"⏸️  Cycle {cycle + 1} complete, pausing before next cycle...")
                await asyncio.sleep(1.0)
        
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"🎉 All {cycles} cycles complete!")
        logger.info("=" * 70)
        await asyncio.sleep(2.0)
        
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"🔌 Client {client_info} disconnected")
    except Exception as e:
        logger.error(f"❌ Error in handler: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def run_server(
    host: str,
    port: int,
    data_file: str,
    cycles: int,
    pause: float,
    bins: str = None
):
    """Run the WebSocket server."""
    # Load data
    logger.info(f"Loading data from {data_file}...")
    all_data, index_map = load_xps_data(data_file)
    
    # Parse bins to send
    bins_to_send = None
    if bins:
        try:
            bins_to_send = [int(b.strip()) for b in bins.split(',')]
            logger.info(f"Will send only bins: {bins_to_send}")
        except:
            logger.error(f"Invalid bins format: {bins}. Using all bins.")
    
    logger.info(f"WebSocket server: ws://{host}:{port}/simImages")
    
    async def handler(websocket):
        await websocket_handler(
            websocket, 
            all_data, 
            index_map, 
            cycles, 
            pause,
            bins_to_send
        )
    
    async with websockets.serve(handler, host, port):
        logger.info("Server running...")
        await asyncio.Future()


@app.command()
def main(
    host: str = typer.Option("0.0.0.0", help="Host to bind"),
    port: int = typer.Option(8001, help="Port to bind"),
    data_file: str = typer.Option(
        "/data/xps_test/combined_all_bin_averages.npy",
        help="Path to combined_all_bin_averages.npy"
    ),
    cycles: int = typer.Option(1, help="Number of cycles to repeat all data"),
    pause: float = typer.Option(0.1, help="Pause between shots (seconds)"),
    bins: str = typer.Option(None, help="Comma-separated bin numbers to send (e.g. '0,1,5')"),
):
    """
    XPS WebSocket Simulator
    
    Loads data from combined_all_bin_averages.npy and streams via WebSocket.
    Compatible with tr_ap_xps message format.
    
    Data structure:
    - Dictionary with keys (bin_num, shot_num) -> 2D numpy array
    - shot_num resets for each bin
    - Sends bins in order, with all shots for each bin
    
    Examples:
        # Send all bins
        python -m arroyosas.app.xps_websocket_simulator --data-file data.npy
        
        # Send only bins 0, 1, and 5
        python -m arroyosas.app.xps_websocket_simulator --bins "0,1,5"
        
        # Repeat 10 times with 0.05s pause
        python -m arroyosas.app.xps_websocket_simulator --cycles 10 --pause 0.05
    """
    logger.info("=" * 60)
    logger.info("XPS WebSocket Simulator (Bin Structure)")
    logger.info("=" * 60)
    logger.info(f"Host: {host}:{port}")
    logger.info(f"Data file: {data_file}")
    logger.info(f"Cycles: {cycles}, Pause: {pause}s")
    if bins:
        logger.info(f"Bins filter: {bins}")
    logger.info("=" * 60)
    
    if not os.path.exists(data_file):
        logger.error(f"Data file not found: {data_file}")
        logger.info("Please provide the path to combined_all_bin_averages.npy")
        return
    
    asyncio.run(run_server(host, port, data_file, cycles, pause, bins))


if __name__ == "__main__":
    app()