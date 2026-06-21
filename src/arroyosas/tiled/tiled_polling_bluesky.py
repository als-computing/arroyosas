"""Polling-based Tiled listener for Bluesky runs, streams, and frame events."""

import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict

from arroyopy.listener import Listener
from tiled.client import from_uri
from tiled.client.base import BaseClient

from arroyosas.schemas import (
    RawFrameEvent,
    SASMessage,
    SASStart,
    SerializableNumpyArrayModel,
)

logger = logging.getLogger(__name__)


class TiledPollingListener(Listener):
    """Polls a Tiled catalog for new runs, namespaces, streams, and frame events.

    Replaces the websocket-subscription approach with periodic HTTP polling so
    that it works against any Tiled server regardless of streaming support.

    Args:
        tiled_client: An authenticated Tiled root client.
        stream_name: Name of the stream to watch for frame data (e.g. ``"primary"``).
        raw_data_path: Slash-separated path within the catalog to poll under.
        target: Key within each stream to watch for frame data (e.g. ``"img"``).
        poll_interval: Seconds between poll cycles.
        lookback_runs: Number of already-existing runs to process on startup.
            ``0`` (default) skips all existing runs and watches only for new ones.
            ``None`` processes every run visible in the initial page.
            A positive integer processes the *n* most-recent existing runs.
        create_run_logs: Whether to write per-event JSON log files.
        log_dir: Root directory for run log folders.
        current_run_dir: Override the active run log directory.
    """

    _PAGE_SIZE: int = 100

    def __init__(
        self,
        tiled_client: BaseClient,
        stream_name: str,
        raw_data_path: str | None = None,
        target: str = "img",
        poll_interval: float = 2.0,
        lookback_runs: int | None = 0,
        create_run_logs: bool = True,
        log_dir: str = "tiled_logs",
        current_run_dir: str | None = None,
    ) -> None:
        self.tiled_client = tiled_client
        self.stream_name = stream_name
        self.raw_data_path = raw_data_path
        self.target = target
        self.poll_interval = poll_interval
        self.lookback_runs = lookback_runs
        self.create_run_logs = create_run_logs
        if create_run_logs:
            os.makedirs(log_dir, exist_ok=True)
        self.log_dir = log_dir
        self.current_run_dir = current_run_dir
        self.event_counters: defaultdict[str, int] = defaultdict(int)

        self._seen_scans: set[str] = set()
        self._seen_namespaces: dict[str, set[str]] = defaultdict(set)
        self._seen_streams: dict[str, set[str]] = defaultdict(set)
        # event_counts[run_uid][stream_name] = last known row count
        self._event_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        self._initialized = False
        self._running = False
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the polling loop."""
        self._running = True
        logger.info("TiledPollingListener starting (interval=%.1fs)", self.poll_interval)
        try:
            await self._poll_loop()
        except KeyboardInterrupt:
            logger.info("TiledPollingListener interrupted by user")
        finally:
            self._running = False
            logger.info("TiledPollingListener stopped")

    async def stop(self) -> None:
        """Stop the polling loop and call the parent stop."""
        self._stop_event.set()
        await super().stop()

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.to_thread(self._poll_once)
            except Exception as e:
                logger.warning("Poll cycle error: %s", e)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    def _poll_once(self) -> None:
        try:
            root = self._root_node()
        except Exception as e:
            logger.warning("Could not reach root node: %s", e)
            return

        if not self._initialized:
            self._initialize_seen_scans(root)
            self._initialized = True

        self._poll_scans(root)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _root_node(self) -> Any:
        node = self.tiled_client
        if self.raw_data_path:
            for part in self.raw_data_path.strip("/").split("/"):
                node = node[part]
        return node

    def _recent_page(self, root: Any, limit: int) -> list[tuple[str, Any]]:
        """Return up to *limit* most-recently-inserted runs as ``(key, node)`` pairs.

        Uses ``root[-limit:None:-1]`` (the syntax required by Tiled Tree sequences
        for negative-start slices), then reverses to restore oldest-first order.
        """
        try:
            sliced = root[-limit:None:-1]
            # Some Tiled versions return a plain list; others return a catalog
            # sub-node that exposes .items().
            if isinstance(sliced, list):
                items = list(sliced)
            else:
                items = list(sliced.items())
            items.reverse()  # restore oldest-first order
            return items
        except Exception as e:
            logger.warning("Recent-page fetch failed: %s", e)
            return []

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize_seen_scans(self, root: Any) -> None:
        """Seed _seen_scans according to lookback_runs before the first poll."""
        page = self._recent_page(root, self._PAGE_SIZE)
        page_keys = [key for key, _ in page]

        if self.lookback_runs == 0:
            self._seen_scans = set(page_keys)
            logger.info(
                "lookback_runs=0: marked %d recent run(s) as seen, watching for new ones",
                len(page_keys),
            )
        elif self.lookback_runs is None:
            logger.info("lookback_runs=None: will process up to %d recent run(s)", len(page_keys))
        else:
            n = max(0, self.lookback_runs)
            skip_keys = page_keys[:-n] if n < len(page_keys) else []
            self._seen_scans = set(skip_keys)
            logger.info(
                "lookback_runs=%d: will process %d recent run(s), skipping %d older",
                n,
                len(page_keys) - len(skip_keys),
                len(skip_keys),
            )

    # ------------------------------------------------------------------
    # Level 1 — scans / runs
    # ------------------------------------------------------------------

    def _poll_scans(self, root: Any) -> None:
        page = self._recent_page(root, self._PAGE_SIZE)
        if not page:
            return

        current_keys = {key for key, _ in page}
        new_keys = current_keys - self._seen_scans

        for key, run_node in page:
            if key in new_keys:
                self._seen_scans.add(key)
                try:
                    self._on_new_run(key, run_node)
                except Exception as e:
                    logger.warning("Error handling new run %s: %s", key, e)
            else:
                try:
                    self._poll_namespaces(key, run_node)
                except Exception as e:
                    logger.warning("Error re-visiting run %s: %s", key, e)

    def _on_new_run(self, uid: str, run_node: Any) -> None:
        scan_id = run_node.metadata.get("scan_id", "N/A")
        logger.info("New run: %s  scan_id: %s", uid, scan_id)

        if self.create_run_logs:
            self._create_run_folder(uid)
            self._log_event("on_new_run", {"uid": uid, "scan_id": scan_id})

        self.publish_start(run_node, {"key": uid})
        self._poll_namespaces(uid, run_node)

    # ------------------------------------------------------------------
    # Level 2 — namespaces
    # ------------------------------------------------------------------

    def _poll_namespaces(self, run_uid: str, run_node: Any) -> None:
        try:
            current_keys = set(run_node.keys())
        except Exception as e:
            logger.warning("Failed to list namespaces for %s: %s", run_uid, e)
            return

        seen = self._seen_namespaces[run_uid]
        for key in current_keys - seen:
            seen.add(key)
            logger.info("New namespace: %s  (run: %s)", key, run_uid)
            if self.create_run_logs:
                self._log_event("on_new_namespace", {"run_uid": run_uid, "namespace": key})

        for key in seen:
            try:
                self._poll_streams(run_uid, key, run_node[key])
            except Exception as e:
                logger.warning("Error visiting namespace %s/%s: %s", run_uid, key, e)

    # ------------------------------------------------------------------
    # Level 3 — streams
    # ------------------------------------------------------------------

    def _poll_streams(self, run_uid: str, ns_key: str, ns_node: Any) -> None:
        try:
            current_keys = set(ns_node.keys())
        except Exception as e:
            logger.warning("Failed to list streams for %s/%s: %s", run_uid, ns_key, e)
            return

        path = f"{run_uid}/{ns_key}"
        seen = self._seen_streams[path]
        for key in current_keys - seen:
            seen.add(key)
            logger.info("New stream: %s  (run: %s  ns: %s)", key, run_uid, ns_key)
            if self.create_run_logs:
                self._log_event("on_new_stream", {"run_uid": run_uid, "namespace": ns_key, "stream": key})

        for key in seen:
            if key != self.stream_name:
                continue
            try:
                self._poll_events(run_uid, key, ns_node[key])
            except Exception as e:
                logger.warning("Error visiting stream %s/%s/%s: %s", run_uid, ns_key, key, e)

    # ------------------------------------------------------------------
    # Level 4 — events
    # ------------------------------------------------------------------

    def _poll_events(self, run_uid: str, stream_name: str, stream_node: Any) -> None:
        try:
            target_keys = set(stream_node.keys())
        except Exception as e:
            logger.warning("Failed to list keys in stream %s/%s: %s", run_uid, stream_name, e)
            return

        if self.target not in target_keys:
            return

        try:
            event_node = stream_node[self.target]
            current_len = len(event_node)
        except Exception as e:
            logger.warning(
                "Failed to read event count for %s/%s/%s: %s",
                run_uid,
                stream_name,
                self.target,
                e,
            )
            return

        last_len = self._event_counts[run_uid][stream_name]
        if current_len <= last_len:
            return

        logger.info(
            "New events in %s/%s: indices %d–%d",
            run_uid,
            stream_name,
            last_len,
            current_len - 1,
        )
        self._event_counts[run_uid][stream_name] = current_len

        for idx in range(last_len, current_len):
            try:
                self._on_new_event(run_uid, stream_name, idx, event_node)
            except Exception as e:
                logger.warning("Error handling event %s/%s[%d]: %s", run_uid, stream_name, idx, e)

    def _on_new_event(self, run_uid: str, stream_name: str, idx: int, event_node: Any) -> None:
        if self.create_run_logs:
            self._log_event("on_event", {"run_uid": run_uid, "stream": stream_name, "index": idx})
        self.publish_event(run_uid, stream_name, idx, event_node)

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def send_to_operator(self, message: SASMessage) -> None:
        asyncio.run(self.publish(message))

    def publish_start(self, run_node: BaseClient, data: Dict[str, Any]) -> None:
        metadata = getattr(run_node, "metadata", {}) or {}
        base_url = getattr(self.tiled_client.context, "base_url", "")
        start = SASStart(
            run_id=data["key"],
            run_name=metadata.get("run_name", data["key"]),
            width=metadata.get("width", 0),
            height=metadata.get("height", 0),
            data_type=metadata.get("data_type", ""),
            tiled_url=str(base_url),
        )
        self.send_to_operator(start)

    def publish_event(self, run_uid: str, stream_name: str, idx: int, event_node: Any) -> None:
        message = RawFrameEvent(
            image=SerializableNumpyArrayModel(array=event_node[idx]),
            frame_number=idx,
            tiled_url="",
        )
        self.send_to_operator(message)

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _create_run_folder(self, run_id: str) -> None:
        run_folder = os.path.join(self.log_dir, f"run_{run_id}")
        os.makedirs(run_folder, exist_ok=True)
        self.current_run_dir = run_folder
        self.event_counters.clear()

    def _log_event(self, event_name: str, data: Dict[str, Any]) -> None:
        """Write a JSON log entry for *event_name* into the current run directory."""
        if self.current_run_dir is None:
            return

        self.event_counters[event_name] += 1
        sequence = self.event_counters[event_name]
        filename = f"{event_name}_{sequence:04d}.json"
        filepath = os.path.join(self.current_run_dir, filename)
        os.makedirs(self.current_run_dir, exist_ok=True)

        log_data = {
            "event_name": event_name,
            "sequence": sequence,
            "timestamp": time.time(),
            **data,
        }
        try:
            with open(filepath, "w") as f:
                json.dump(log_data, f, indent=2, default=str)
            logger.debug("Logged %s to %s", event_name, filepath)
        except Exception as e:
            logger.error("Failed to log event %s: %s", event_name, e)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def tiled_ws_listener_factory(
    uri: str,
    stream_name: str,
    operator=None,
    raw_data_path: str | None = None,
    target: str = "img",
    poll_interval: float = 2.0,
    lookback_runs: int | None = 0,
    create_run_logs: bool = True,
    log_dir: str = "tiled_logs",
    api_key: str | None = None,
    current_run_dir: str | None = None,
) -> TiledPollingListener:
    """Create a TiledPollingListener connected to the given URI.

    Args:
        uri: Tiled server URI.
        stream_name: Stream to watch for frame data (e.g. ``"primary"``).
        operator: Unused; kept for API compatibility with the websocket factory.
        raw_data_path: Path within the catalog to poll under.
        target: Key within each stream to watch for frame data.
        poll_interval: Seconds between poll cycles.
        lookback_runs: Existing runs to process on startup (0 = new only, None = all).
        create_run_logs: Whether to write per-event JSON log files.
        log_dir: Root directory for run log folders.
        api_key: Tiled API key; falls back to TILED_LIVE_API_KEY or TILED_API_KEY env vars.
        current_run_dir: Override the active run log directory.

    Returns:
        A configured TiledPollingListener ready to start.
    """
    if api_key is None:
        api_key = os.environ.get("TILED_LIVE_API_KEY") or os.environ.get("TILED_API_KEY")
    client = from_uri(uri, api_key=api_key)
    return TiledPollingListener(
        tiled_client=client,
        stream_name=stream_name,
        raw_data_path=raw_data_path,
        target=target,
        poll_interval=poll_interval,
        lookback_runs=lookback_runs,
        create_run_logs=create_run_logs,
        log_dir=log_dir,
        current_run_dir=current_run_dir,
    )


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    # Suppress httpx by default; enable with --httpx-debug.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Poll a Tiled catalog for new scan data.")
    parser.add_argument("--uri", default="https://tiled.nsls2.bnl.gov")
    parser.add_argument("--path", default="smi/migration")
    parser.add_argument("--stream", default="primary")
    parser.add_argument("--target", default="img")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument(
        "--lookback",
        type=int,
        default=0,
        metavar="N",
        help="Process the N most-recent existing runs on startup (0 = new runs only).",
    )
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging for this module.")
    parser.add_argument("--httpx-debug", action="store_true", help="Enable DEBUG logging for httpx/httpcore.")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    if args.httpx_debug:
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)

    listener = tiled_ws_listener_factory(
        uri=args.uri,
        stream_name=args.stream,
        raw_data_path=args.path,
        target=args.target,
        poll_interval=args.interval,
        lookback_runs=args.lookback,
    )
    asyncio.run(listener.start())
