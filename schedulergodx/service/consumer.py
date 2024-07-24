import json
from typing import Any, Callable, Mapping, NoReturn

import schedulergodx.utils as utils


class Consumer(utils.AbstractionConnectClass):
    
    def start_consuming(self, on_message: Callable) -> NoReturn:  # type: ignore[misc]
        self.channel.basic_consume(self.queue, on_message)
        self.channel.start_consuming()


        
    