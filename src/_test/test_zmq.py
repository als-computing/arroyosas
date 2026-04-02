"""Tests for arroyosas.zmq (ZMQFrameListener, ZMQFramePublisher, ZMQBroker)"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import msgpack
import numpy as np
import pytest

from arroyosas.schemas import RawFrameEvent, SASStart, SASStop, SerializableNumpyArrayModel
from arroyosas.zmq import ZMQBroker, ZMQFrameListener, ZMQFramePublisher, create_zmq_frame_listener

pytestmark = pytest.mark.asyncio


def _make_raw_frame_event():
    image = SerializableNumpyArrayModel(array=np.zeros((10, 10), dtype=np.float32))
    return RawFrameEvent(image=image, frame_number=0, tiled_url="http://example.com")


def _make_start_event():
    return SASStart(
        run_name="run1",
        run_id="id1",
        width=10,
        height=10,
        data_type="float32",
        tiled_url="http://example.com",
    )


def _make_stop_event():
    return SASStop(num_frames=5)


class TestZMQFrameListener:
    async def test_start_handles_start_message(self):
        operator = AsyncMock()
        zmq_socket = AsyncMock()

        start = _make_start_event()
        packed = msgpack.packb(start.model_dump(), use_bin_type=True)

        call_count = 0

        async def fake_recv():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return packed
            raise asyncio.CancelledError()

        zmq_socket.recv = fake_recv

        listener = ZMQFrameListener(operator, zmq_socket)
        with pytest.raises(asyncio.CancelledError):
            await listener.start()

        operator.process.assert_called_once()
        called_with = operator.process.call_args[0][0]
        assert isinstance(called_with, SASStart)

    async def test_start_handles_event_message(self):
        operator = AsyncMock()
        zmq_socket = AsyncMock()

        event = _make_raw_frame_event()
        dumped = event.model_dump()
        packed = msgpack.packb(dumped, use_bin_type=True)

        call_count = 0

        async def fake_recv():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return packed
            raise asyncio.CancelledError()

        zmq_socket.recv = fake_recv
        listener = ZMQFrameListener(operator, zmq_socket)
        with pytest.raises(asyncio.CancelledError):
            await listener.start()

        operator.process.assert_called_once()
        called_with = operator.process.call_args[0][0]
        assert isinstance(called_with, RawFrameEvent)

    async def test_start_handles_stop_message(self):
        operator = AsyncMock()
        zmq_socket = AsyncMock()

        stop = _make_stop_event()
        packed = msgpack.packb(stop.model_dump(), use_bin_type=True)

        call_count = 0

        async def fake_recv():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return packed
            raise asyncio.CancelledError()

        zmq_socket.recv = fake_recv
        listener = ZMQFrameListener(operator, zmq_socket)
        with pytest.raises(asyncio.CancelledError):
            await listener.start()

        operator.process.assert_called_once()
        called_with = operator.process.call_args[0][0]
        assert isinstance(called_with, SASStop)

    async def test_start_handles_unknown_message(self):
        operator = AsyncMock()
        zmq_socket = AsyncMock()

        unknown = {"msg_type": "unknown_type", "data": "something"}
        packed = msgpack.packb(unknown, use_bin_type=True)

        call_count = 0

        async def fake_recv():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return packed
            raise asyncio.CancelledError()

        zmq_socket.recv = fake_recv
        listener = ZMQFrameListener(operator, zmq_socket)
        with pytest.raises(asyncio.CancelledError):
            await listener.start()

        # Unknown type - operator.process should NOT have been called
        operator.process.assert_not_called()

    async def test_start_handles_exception_gracefully(self):
        operator = AsyncMock()
        zmq_socket = AsyncMock()

        call_count = 0

        async def fake_recv():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Socket error")
            raise asyncio.CancelledError()

        zmq_socket.recv = fake_recv
        listener = ZMQFrameListener(operator, zmq_socket)
        with pytest.raises(asyncio.CancelledError):
            await listener.start()

        # Exception was caught, no crash, no process call
        operator.process.assert_not_called()

    async def test_stop(self):
        operator = AsyncMock()
        zmq_socket = AsyncMock()
        listener = ZMQFrameListener(operator, zmq_socket)
        # stop() is a no-op
        await listener.stop()

    def test_from_settings(self):
        operator = MagicMock()
        settings = MagicMock()
        settings.zmq_address = "tcp://localhost:5555"
        listener = ZMQFrameListener.from_settings(settings, operator)
        assert isinstance(listener, ZMQFrameListener)


class TestZMQFramePublisher:
    async def test_publish_start(self):
        zmq_socket = AsyncMock()
        publisher = ZMQFramePublisher(zmq_socket)
        start = _make_start_event()
        await publisher.publish(start)
        zmq_socket.send.assert_called_once()

    async def test_publish_stop(self):
        zmq_socket = AsyncMock()
        publisher = ZMQFramePublisher(zmq_socket)
        stop = _make_stop_event()
        await publisher.publish(stop)
        zmq_socket.send.assert_called_once()

    async def test_publish_raw_frame_event(self):
        zmq_socket = AsyncMock()
        publisher = ZMQFramePublisher(zmq_socket)
        event = _make_raw_frame_event()
        await publisher.publish(event)
        zmq_socket.send.assert_called_once()

    async def test_publish_unknown_type_logs_warning(self):
        zmq_socket = AsyncMock()
        publisher = ZMQFramePublisher(zmq_socket)
        # Unknown message type
        msg = MagicMock()
        msg.msg_type = "unknown"
        await publisher.publish(msg)
        zmq_socket.send.assert_not_called()

    def test_from_settings(self):
        settings = MagicMock()
        settings.address = "tcp://localhost:5556"
        with patch("arroyosas.zmq.Context") as mock_context:
            mock_ctx = MagicMock()
            mock_context.return_value = mock_ctx
            mock_socket = MagicMock()
            mock_ctx.socket.return_value = mock_socket
            pub = ZMQFramePublisher.from_settings(settings)
            assert isinstance(pub, ZMQFramePublisher)


class TestZMQBroker:
    def test_init(self):
        broker = ZMQBroker("tcp://localhost:5555", "tcp://localhost:5556", 1000)
        assert broker.zmq_dealer_address == "tcp://localhost:5555"
        assert broker.zmq_router_address == "tcp://localhost:5556"
        assert broker.router_hwm == 1000

    async def test_start_calls_proxy(self):
        broker = ZMQBroker("tcp://localhost:5555", "tcp://localhost:5556", 1000)

        with patch("arroyosas.zmq.zmq.asyncio.Context") as mock_context, patch(
            "arroyosas.zmq.zmq.proxy"
        ) as mock_proxy:
            mock_ctx = MagicMock()
            mock_context.return_value = mock_ctx
            mock_router = MagicMock()
            mock_dealer = MagicMock()
            mock_ctx.socket.side_effect = [mock_router, mock_dealer]

            await broker.start()
            mock_proxy.assert_called_once_with(mock_router, mock_dealer)

    def test_from_settings(self):
        settings = MagicMock()
        settings.dealer_address = "tcp://localhost:5555"
        settings.router_address = "tcp://localhost:5556"
        settings.router_hwm = 500
        broker = ZMQBroker.from_settings(settings)
        assert broker.zmq_dealer_address == "tcp://localhost:5555"
        assert broker.zmq_router_address == "tcp://localhost:5556"
        assert broker.router_hwm == 500


class TestCreateZmqFrameListener:
    def test_create_zmq_frame_listener(self):
        operator = MagicMock()
        with patch("arroyosas.zmq.Context") as mock_context:
            mock_ctx = MagicMock()
            mock_context.return_value = mock_ctx
            mock_socket = MagicMock()
            mock_ctx.socket.return_value = mock_socket

            listener = create_zmq_frame_listener(operator, "tcp://localhost:5557")

            assert isinstance(listener, ZMQFrameListener)
            mock_socket.connect.assert_called_once_with("tcp://localhost:5557")
