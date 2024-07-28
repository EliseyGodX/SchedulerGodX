import json
from typing import Callable, NoReturn
import asyncio
import schedulergodx.utils as utils
from schedulergodx.service.publisher import Publisher


class Consumer(utils.AbstractionConnectClass):
    
    def start_consuming(self, on_message: Callable) -> NoReturn:  # type: ignore[misc]
        self.channel.basic_consume(self.queue, on_message)
        self.channel.start_consuming()