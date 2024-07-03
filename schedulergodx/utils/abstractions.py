import json
from abc import ABC
from logging import Logger
from typing import Any, Mapping, NoReturn, Sequence

import pika

from schedulergodx.utils.logger import LoggerConstructor


class AbstractionConnectClass(ABC):
    
    def __init__(self, name: str, *, logger: Logger, rmq_parametrs: Mapping[str, Any], 
                  rmq_credentials: Sequence[str], rmq_que: str):
        self.name = name
        self.logger = logger
        self.queue = rmq_que
        self.channel = self._rmq_init(rmq_parametrs, rmq_credentials, rmq_que)
        self._logging('info', 'successful initialization')
        
    def _rmq_init(self, rmq_parametrs, rmq_credentials, rmq_que):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(**rmq_parametrs, credentials=pika.PlainCredentials(*rmq_credentials)))
            channel = connection.channel()
            channel.queue_declare(queue=rmq_que, durable=True)
            self._logging('debug', f'connected to RabbitMQ (queue: {rmq_que})')         
            return channel
        except pika.exceptions.AMQPConnectionError as e:
            self._logging('fatal', f'failed to connect to RabbitMQ (queue: {rmq_que}): {e}')
            raise e
        
    def _logging(self, level: str, message: str):
        LoggerConstructor.log_levels(self.logger)[level](f'{self.name} - {message}')