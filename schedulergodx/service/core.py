from ast import match_case
import asyncio
import base64
from curses import meta
import json
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from logging import Logger
from typing import Any, Callable, Generator, Mapping, NoReturn, Sequence

import dill

import schedulergodx.utils as utils
from schedulergodx.service.consumer import Consumer
from schedulergodx.service.publisher import Publisher


@dataclass
class Service:
    name: str = 'service'
    active_client: Sequence[str] = field(default_factory=lambda: [utils.AllClients()])
    log_name: str = 'core'
    rmq_parameters: Mapping[str, Any] = field(default_factory=dict)
    rmq_credentials: Sequence[str] = field(default_factory=list)
    rmq_publisher_que: str = 'service-client'
    rmq_consumer_que: str = 'client-service'
    id_generator: Generator = utils.ulid_generator()
    logger: Logger = utils.LoggerConstructor(name=name).getLogger()

    def __post_init__(self) -> None:
        connection_kwargs = {'logger': self.logger, 'rmq_parametrs': self.rmq_parameters, 
                             'rmq_credentials': self.rmq_credentials}
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, **connection_kwargs)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, **connection_kwargs)
        self._logging('info', f'successful initialization')
    
    @staticmethod
    def time(**kwargs) -> timedelta:
        return timedelta(**kwargs)
    
    def _logging(self, level: str, message: str) -> None:
        utils.LoggerConstructor.log_levels(self.logger)[level](f'{self.log_name} - {message}')
        
    def start(self) -> NoReturn:
        def on_message(channel, method_frame, header_frame, body) -> None:
            try:
                body = json.loads(body)
                metadata = {
                    'id': body['id'],
                    'client': body['client'],
                    'type': body['type']
                }
                data = body.get('arguments')
            except json.JSONDecodeError:
                return self._logging('info', 'a non-json message was received')
            except KeyError:
                return self._logging('info', 
                    'the received message has an incorrect format (correct format: {"id": ..., "client": ..., "type": ..., "arguments": ...}')
                
            if metadata['client'] not in self.active_client:
                return self._logging('info', 'a message has been received from an unregistered client')
            
            match metadata['type']:
                case utils.Message.INFO.value:
                    pass
                
                case utils.Message.TASK.value:
                    pass
                
                case utils.Message.DELAYED_TASK.value:
                    pass
                
                case _:
                    self._logging('info', utils.Error.IncorrectType.value)
                    self.publisher.publish(data={
                        'id': metadata['id'],
                        'type': utils.Message.INFO.value,
                        'client': metadata['client'],
                        'arguments': utils.Error.IncorrectType.value
                    })
                        
        self.consumer.start_consuming(on_message)