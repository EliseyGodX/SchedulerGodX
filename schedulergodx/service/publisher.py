import json
from typing import Mapping

import pika
import schedulergodx.utils as utils

class Publisher(utils.AbstractionConnectClass):
    
    def publish(self, data: Mapping, delivery_mode: int = 2):
        self.channel.basic_publish(
            exchange='',
            routing_key='service-client',
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode,
            ))
        self._logging('info', f'successfully published message ({data.get("id")}) to queue "{self.queue}"')
        
    