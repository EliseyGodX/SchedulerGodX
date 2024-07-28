from functools import cached_property
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import Logger
from typing import Any, Generator, MutableSequence, NoReturn, Optional

import schedulergodx.utils as utils
from schedulergodx.service.consumer import Consumer
from schedulergodx.service.publisher import Publisher
from schedulergodx.utils.logger import LoggerConstructor
from schedulergodx.utils.storage import DB


class _Client:
    
    def __init__(self, name: str, *, enable_overdue: bool = False) -> None:
        self.name = name
        self.enable_overdue = enable_overdue
    
    def __repr__(self) -> str:
        return self.name
        
    def __eq__(self, another_client) -> bool:
        return self.name == another_client
    
    
class Task:
    
    def __init__(self, id: str, client: str, type: utils.Message, delay: Optional[int], task) -> None:
        ...
        

@dataclass
class Service(utils.AbstractionCore):
    name: str = 'service'
    active_client: MutableSequence[_Client] = field(
        default_factory=lambda: []
        )
    db: DB = DB(service_db=True)
    
    def __post_init__(self) -> None:
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, 
                                   logger=self.logger, rmq_connect=self.rmq_connect)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, 
                                 logger=self.logger, rmq_connect=self.rmq_connect)
        self._logging('info', f'successful initialization')
    
    @property
    def rmq_publisher_que(self) -> str:
        return 'service-client'
    
    @property
    def rmq_consumer_que(self) -> str:
       return 'client-service'
   
    @cached_property
    def logger(self) -> Logger:
       return LoggerConstructor(name=self.name).getLogger()
   
    def _error_message(self, message_id: utils.MessageId, client: str, 
                       error: utils.MessageErrorStatus, error_message: str) -> None:
        self.publisher.publish(data=utils.MessageConstructor.error(
            id_ = message_id, client = client,
            error = error, message = error_message
        ))
        self._logging('error', f'Error {error} (message id: {message_id}, client: {client})')
        
    def add_active_client(self, client: _Client) -> None:
        self.active_client.append(client)
        self.db.add_client(client.__dict__)
        self._logging('info', f'new client ({client})')
        
    def start(self, enable_overdue: bool = False) -> NoReturn:
        ...
        
    def _start_consuming(self) -> NoReturn:
        def on_message(channel, method_frame, header_frame, body) -> None:
            channel.basic_ack(method_frame.delivery_tag)
            try:
                message = utils.MessageConstructor.disassemble(body)
            except json.JSONDecodeError:
                return self._logging('error', 'a non-json message was received')
            except KeyError:
                return self._logging('error', 'the received message has an incorrect format')
                
            if (message.metadata['client'] not in self.active_client 
                and message.metadata['type'] != utils.Message.INITIALIZATION):
                return self._error_message(
                    message_id = message.metadata['id'], 
                    client = message.metadata['client'],
                    error = utils.MessageErrorStatus.UNREGGISTERED_CLIENT,
                    error_message = 'a message has been received from an unregistered client'
                )
                            
            match message.metadata['type']:
                
                case utils.Message.INITIALIZATION:
                    try:
                        client = _Client(message.metadata['client'], **message.arguments)
                    except TypeError:
                        return self._error_message(
                            message_id = message.metadata['id'],
                            client = message.metadata['client'],
                            error = utils.MessageErrorStatus.BAD_INITIALIZATION,
                            error_message = 'incorrect client parameters'
                        )
                    if client in self.active_client: return self._error_message(
                        message_id = message.metadata['id'],
                        client = message.metadata['client'],
                        error = utils.MessageErrorStatus.DUPLICAT_NAME,
                        error_message = 'a client with this name exists'
                    )
                    self.add_active_client(client)   
                    self.publisher.publish(data = utils.MessageConstructor.info(
                        id_ = message.metadata['id'],
                        client = message.metadata['client'],
                        responce = utils.MessageInfoStatus.OK.value
                    ))
                
                case utils.Message.INFO: ...
                
                case utils.Message.TASK: ...
                
                case _:
                     return self._error_message(
                        message_id = message.metadata['id'],
                        client = message.metadata['client'],
                        error = utils.MessageErrorStatus.INCORRECT_TYPE,
                        error_message = 'invalid message type received'
                    )
                    
        self.consumer.start_consuming(on_message)
        
    def get_delay(self, time: datetime) -> int: 
        delay = int(
            (time - datetime.now())
            .total_seconds())
        if delay < 0: delay = 0
        return delay

    def timings(self):
        pass
        
