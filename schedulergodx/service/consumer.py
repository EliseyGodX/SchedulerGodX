from typing import Callable, NoReturn

import schedulergodx.utils as utils


class Consumer(utils.AbstractionConnectClass):
    
    def start_consuming(self, on_message: Callable) -> NoReturn: 
        self.channel.basic_consume(self.queue, on_message)
        self.channel.start_consuming()