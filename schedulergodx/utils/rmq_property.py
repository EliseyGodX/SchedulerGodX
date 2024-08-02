from collections import namedtuple
from typing import Any, Mapping, Sequence

import pika
from pika.adapters.blocking_connection import BlockingChannel

RmqSettings = namedtuple('RmqSettings', 'parametrs credentials')
rmq_default_settings = RmqSettings(
    {
        'host': 'localhost',
        'port': 5672,
        'virtual_host': '/',
        'heartbeat': 600, 
        'blocked_connection_timeout': 300
    },
    (
        'guest', 
        'guest'
    )
)

class RmqConnect:
    
    def __init__(self, rmq_parameters: Mapping[str, Any], 
                  rmq_credentials: Sequence[str]) -> None:
        self.rmq_parameters = rmq_parameters
        self.rmq_credentials = rmq_credentials
        
    def get_channel(self, queue: str) -> BlockingChannel:
        connection = (
            pika.BlockingConnection(
                pika.ConnectionParameters(
                    **self.rmq_parameters, credentials=pika.PlainCredentials(
                        *self.rmq_credentials
                        )
                    )
                )
            )
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)         
        return channel