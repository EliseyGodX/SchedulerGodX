import json
from dataclasses import dataclass, field
from datetime import timedelta
from logging import Logger
from typing import Any, Generator, Mapping, NoReturn, Sequence, MutableSequence
from uu import Error

import schedulergodx.utils as utils
from schedulergodx.service.consumer import Consumer
from schedulergodx.service.publisher import Publisher


@dataclass
class Service:
    name: str = 'service'
    active_client: MutableSequence[str | utils.AllClients] = field(
        default_factory=lambda: [utils.AllClients()]
        )
    log_name: str = 'core'
    rmq_connect: utils.RmqConnect = utils.RmqConnect(
        rmq_parameters=utils.rmq_default_settings.parametrs,
        rmq_credentials=utils.rmq_default_settings.credentials
    )
    rmq_publisher_que: str = 'service-client'
    rmq_consumer_que: str = 'client-service'
    id_generator: Generator[utils.MessageId, None, NoReturn] = utils.ulid_generator()
    logger: Logger = utils.LoggerConstructor(name=name).getLogger()

    class MessageHandler:
        
        @staticmethod
        def info(): ...
        
        @staticmethod
        def task(): ...
        
        @staticmethod
        def delayed_task(): ...
        
    def __post_init__(self) -> None:
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, 
                                   logger=self.logger, rmq_connect=self.rmq_connect)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, 
                                 logger=self.logger, rmq_connect=self.rmq_connect)
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
                    self.MessageHandler.info()
                
                case utils.Message.TASK.value:
                    self.MessageHandler.task()
                
                case utils.Message.DELAYED_TASK.value:
                    self.MessageHandler.delayed_task()
                
                case _:
                    self._logging('info', utils.Error.IncorrectType.value)
                    self.publisher.publish(data={
                        'id': metadata['id'],
                        'type': utils.Message.INFO.value,
                        'client': metadata['client'],
                        'arguments': (Error.__name__, utils.Error.IncorrectType.value, )
                    })
                        
        self.consumer.start_consuming(on_message)