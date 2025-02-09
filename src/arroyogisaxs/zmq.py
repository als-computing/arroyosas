from arroyopy.listener import Listener
from arroyopy.publisher import Publisher

from .schemas import GISAXSMessage


class ZMQFrameListener(Listener):
    async def start(self):
        pass

    async def stop(self):
        pass

    async def listen(self):
        pass


class ZMQFramePublisher(Publisher):
    async def start(self):
        pass

    async def publish(self, message: GISAXSMessage) -> None:
        pass
