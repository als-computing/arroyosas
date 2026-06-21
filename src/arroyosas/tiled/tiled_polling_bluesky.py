"""Polling-based Tiled listener for new scans, namespaces, streams, and events."""

import asyncio
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Any, Callable

from tiled.client import from_uri
from tiled.client.base import BaseClient

logger = logging.getLogger(__name__)


class TiledPoller:
    """Polls a Tiled catalog for new scans, namespaces, streams, and events.

    Rather than subscribing to websocket events, this class periodically walks
    the Tiled node tree and detects newly-appeared keys at each level. Callbacks
    fire once per new item, matching the semantics of the original listener.

    Args:
        tiled_client: An authenticated Tiled root client.
        raw_data_path: Slash-separated path within the catalog to poll under.
        stream_name: Name of the namespace/stream to watch (e.g. ``"primary"``).
            Only namespaces whose key matches this value are descended into.
        target: Key within each stream to watch for frame data (e.g. ``"img"``).
        poll_interval: Seconds between polls at each level.
        lookback_runs: Number of already-existing runs to process before watching
            for new ones. ``0`` (default) skips all existing runs and only reacts
            to runs that appear after the poller starts. ``None`` processes every
            existing run. A positive integer processes the *n* most-recent runs.
        on_new_event: Optional callable invoked with ``(run_uid, stream_name,
            event_index, event_node)`` for each new event row.
    """

    def __init__(
        self,
        tiled_client: BaseClient,
        raw_data_path: str | None = None,
        stream_name: str = "primary",
        target: str = "img",
        poll_interval: float = 2.0,
        lookback_runs: int | None = 0,
        on_new_event: Callable[[str, str, int, Any], None] | None = None,
    ) -> None:
        self.tiled_client = tiled_client
        self.raw_data_path = raw_data_path
        self.stream_name = stream_name
        self.target = target
        self.poll_interval = poll_interval
        self.lookback_runs = lookback_runs
        self.on_new_event = on_new_event

        self._seen_scans: set[str] = set()
        self._seen_namespaces: dict[str, set[str]] = defaultdict(set)
        self._seen_streams: dict[str, set[str]] = defaultdict(set)
        self._event_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        self._initialized = False
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the polling loop in a background thread and block until stopped."""
        self._running = True
        logger.info("TiledPoller starting (interval=%.1fs)", self.poll_interval)
        await asyncio.to_thread(self._run)

    def _run(self) -> None:
        """Blocking poll loop — runs in a thread via asyncio.to_thread."""
        try:
            while self._running:
                self._poll_once()
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info("TiledPoller interrupted by user")
        finally:
            logger.info("TiledPoller stopped")

    def stop(self) -> None:
        """Signal the polling loop to exit."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _root_node(self) -> Any:
        """Return the catalog node to poll under, honoring raw_data_path."""
        node = self.tiled_client
        if self.raw_data_path:
            for part in self.raw_data_path.strip("/").split("/"):
                node = node[part]
        return node

    def _poll_once(self) -> None:
        """Execute one full tree walk synchronously (called in a thread)."""
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
    # Initialization — seed _seen_scans according to lookback_runs
    # ------------------------------------------------------------------

    _PAGE_SIZE: int = 100  # maximum runs fetched per poll cycle

    def _recent_page(self, root: Any, limit: int) -> list[tuple[str, Any]]:
        """Return up to *limit* of the most-recently-inserted runs as ``(key, node)`` pairs.

        Uses ``root[-limit:None:-1]`` — the slice syntax required by Tiled Tree
        sequences for negative-start slices — then reverses the result so the
        returned list is in oldest-first (catalog) order.

        Args:
            root: The catalog node to page through.
            limit: Maximum number of items to return.

        Returns:
            List of ``(key, node)`` pairs in oldest-first order within the window.
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

    def _initialize_seen_scans(self, root: Any) -> None:
        """Seed _seen_scans so that lookback behaviour is respected on first poll.

        Fetches at most one page of the most-recent runs; never walks the full
        catalog.

        * ``lookback_runs=0`` (default): marks the entire page as seen so only
          future runs fire callbacks.
        * ``lookback_runs=n``: marks the oldest page entries as seen, leaving the
          *n* most-recent unseen so they replay immediately.
        * ``lookback_runs=None``: leaves ``_seen_scans`` empty so every run in the
          page (and any subsequent runs) fires callbacks.
        """
        page = self._recent_page(root, self._PAGE_SIZE)
        page_keys = [key for key, _ in page]

        if self.lookback_runs == 0:
            self._seen_scans = set(page_keys)
            logger.info(
                "lookback_runs=0: marked %d recent run(s) as seen, watching for new ones",
                len(page_keys),
            )
        elif self.lookback_runs is None:
            logger.info(
                "lookback_runs=None: will process up to %d recent run(s)",
                len(page_keys),
            )
        else:
            n = max(0, self.lookback_runs)
            skip_keys = page_keys[:-n] if n < len(page_keys) else []
            self._seen_scans = set(skip_keys)
            live_count = len(page_keys) - len(skip_keys)
            logger.info(
                "lookback_runs=%d: will process %d recent run(s), skipping %d older",
                n,
                live_count,
                len(skip_keys),
            )

    # ------------------------------------------------------------------
    # Level 1 — scans
    # ------------------------------------------------------------------

    def _poll_scans(self, root: Any) -> None:
        """Detect new scan (run) keys in the most-recent page of the root node."""
        page = self._recent_page(root, self._PAGE_SIZE)
        if not page:
            return

        current_keys = {key for key, _ in page}
        new_keys = current_keys - self._seen_scans

        for key, run_node in page:
            if key in new_keys:
                self._seen_scans.add(key)
                try:
                    scan_id = run_node.metadata.get("scan_id", "N/A")
                    logger.info("New scan: %s  scan_id: %s", key, scan_id)
                    self._poll_namespaces(key, run_node)
                except Exception as e:
                    logger.warning("Error handling new scan %s: %s", key, e)
            else:
                try:
                    self._poll_namespaces(key, run_node)
                except Exception as e:
                    logger.warning("Error re-visiting scan %s: %s", key, e)

    # ------------------------------------------------------------------
    # Level 2 — namespaces (e.g. "streams", "config", …)
    # ------------------------------------------------------------------

    def _poll_namespaces(self, run_uid: str, run_node: Any) -> None:
        """Detect new namespace keys under a run node."""
        try:
            current_keys = set(run_node.keys())
        except Exception as e:
            logger.warning("Failed to list namespace keys for %s: %s", run_uid, e)
            return

        seen = self._seen_namespaces[run_uid]
        new_keys = current_keys - seen
        for key in sorted(new_keys):
            seen.add(key)
            logger.info("New namespace: %s  (run: %s)", key, run_uid)

        for key in seen:
            if key != self.stream_name:
                continue
            try:
                ns_node = run_node[key]
                self._poll_streams(run_uid, key, ns_node)
            except Exception as e:
                logger.warning("Error visiting namespace %s/%s: %s", run_uid, key, e)

    # ------------------------------------------------------------------
    # Level 3 — streams (e.g. "primary", "baseline", …)
    # ------------------------------------------------------------------

    def _poll_streams(self, run_uid: str, ns_key: str, ns_node: Any) -> None:
        """Detect new stream keys under a namespace node."""
        try:
            current_keys = set(ns_node.keys())
        except Exception as e:
            logger.warning("Failed to list stream keys for %s/%s: %s", run_uid, ns_key, e)
            return

        path = f"{run_uid}/{ns_key}"
        seen = self._seen_streams[path]
        new_keys = current_keys - seen
        for key in sorted(new_keys):
            seen.add(key)
            logger.info("New stream: %s  (run: %s  ns: %s)", key, run_uid, ns_key)

        for key in seen:
            try:
                stream_node = ns_node[key]
                self._poll_events(run_uid, key, stream_node)
            except Exception as e:
                logger.warning("Error visiting stream %s/%s/%s: %s", run_uid, ns_key, key, e)

    # ------------------------------------------------------------------
    # Level 4 — events (rows inside the target array node)
    # ------------------------------------------------------------------

    def _poll_events(self, run_uid: str, stream_name: str, stream_node: Any) -> None:
        """Detect new events in the target array node inside a stream."""
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
        if current_len > last_len:
            new_indices = range(last_len, current_len)
            logger.info(
                "New events in %s/%s: indices %d–%d",
                run_uid,
                stream_name,
                last_len,
                current_len - 1,
            )
            self._event_counts[run_uid][stream_name] = current_len

            if self.on_new_event is not None:
                for idx in new_indices:
                    try:
                        self.on_new_event(run_uid, stream_name, idx, event_node)
                    except Exception as e:
                        logger.warning(
                            "on_new_event callback raised for %s/%s[%d]: %s",
                            run_uid,
                            stream_name,
                            idx,
                            e,
                        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def tiled_poller_factory(
    uri: str,
    raw_data_path: str | None = None,
    stream_name: str = "primary",
    target: str = "img",
    poll_interval: float = 2.0,
    lookback_runs: int | None = 0,
    api_key: str | None = None,
    on_new_event: Callable[[str, str, int, Any], None] | None = None,
) -> TiledPoller:
    """Create a TiledPoller connected to the given URI.

    Args:
        uri: Tiled server URI.
        raw_data_path: Path within the catalog to poll under.
        stream_name: Name of the namespace/stream to watch (e.g. ``"primary"``).
        target: Key within each stream to watch for frame data.
        poll_interval: Seconds between polls.
        lookback_runs: Number of already-existing runs to process on startup.
            ``0`` (default) skips all existing runs. ``None`` processes all of
            them. A positive integer processes the *n* most-recent runs.
        api_key: Tiled API key; falls back to TILED_LIVE_API_KEY or TILED_API_KEY env vars.
        on_new_event: Optional callback ``(run_uid, stream_name, idx, event_node)``.

    Returns:
        A configured TiledPoller ready to start.
    """
    if api_key is None:
        api_key = os.environ.get("TILED_LIVE_API_KEY") or os.environ.get("TILED_API_KEY")
    client = from_uri(uri, api_key=api_key)
    return TiledPoller(
        tiled_client=client,
        raw_data_path=raw_data_path,
        stream_name=stream_name,
        target=target,
        poll_interval=poll_interval,
        lookback_runs=lookback_runs,
        on_new_event=on_new_event,
    )


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------


def _example_event_handler(run_uid: str, stream_name: str, idx: int, event_node: Any) -> None:
    """Example callback: log the shape of each new frame."""
    try:
        frame = event_node[idx]
        logger.info(
            "Frame %d in %s/%s — shape: %s dtype: %s",
            idx,
            run_uid,
            stream_name,
            getattr(frame, "shape", "?"),
            getattr(frame, "dtype", "?"),
        )
    except Exception as e:
        logger.warning("Could not read frame %d: %s", idx, e)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("numexpr").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Poll a Tiled catalog for new scan data.")
    parser.add_argument("--uri", default="https://tiled.nsls2.bnl.gov")
    parser.add_argument("--path", default="smi/migration")
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

    poller = tiled_poller_factory(
        uri=args.uri,
        raw_data_path=args.path,
        target=args.target,
        poll_interval=args.interval,
        lookback_runs=args.lookback,
        on_new_event=_example_event_handler,
    )
    asyncio.run(poller.start())
