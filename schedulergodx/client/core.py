import asyncio
import base64
from functools import cached_property
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import Logger
from typing import (Any, Callable, Generator, Mapping, MutableMapping,
                    NoReturn, Optional, Sequence, TypeAlias)

import dill

import schedulergodx.utils as utils
from schedulergodx.client.consumer import Consumer
from schedulergodx.client.publisher import Publisher
from schedulergodx.utils.logger import LoggerConstructor


@dataclass
class Client(utils.AbstractionCore):
    name: str = 'client'
    task_lifetime: int = 3
    hard_task_lifetime: int = 10
    enable_overdue: bool = False
    
    def __post_init__(self) -> None:
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, 
                                   logger=self.logger, rmq_connect=self.rmq_connect)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, 
                                 logger=self.logger, rmq_connect=self.rmq_connect)
        id_ = next(self.id_generator)
        self.push(data=utils.MessageConstructor.initialization(
            id_ = id_, client = self.name,
            enable_overdue = self.enable_overdue
        ))
        responce = utils.MessageConstructor.disassemble(
            self.sync_await_responce(id_))
        if responce.metadata['type'] == utils.Message.ERROR:
            error = f'{responce.arguments["error_code"]} - {responce.arguments["message"]}'
            self._logging('fatal', error)
            raise error
        elif responce.arguments['responce'] == utils.MessageInfoStatus.OK.value:
            self._logging('info', 'successful initialization')
        else: 
            raise responce
        
    @property
    def rmq_publisher_que(self) -> str:
        return 'client-service'
    
    @property
    def rmq_consumer_que(self) -> str:
       return 'service-client'
   
    @cached_property
    def logger(self) -> Logger:
       return LoggerConstructor(name=self.name).getLogger()
   
    @staticmethod
    def time(**kwargs) -> timedelta:
        return timedelta(**kwargs)
    
    def task(self, func: Callable):
        class Task:
            def __init__(self, func: Callable, client: Client, 
                         delay: Optional[timedelta] = None, hard: bool = False) -> None:
                self.func = func
                self.client = client
                self.task_lifetime = client.task_lifetime
                self.hard_task_lifetime = client.hard_task_lifetime
                self.delay = delay
                self.hard = hard
                
            def set_parametrs(self, **kwargs) -> None:
                self.__dict__.update(kwargs)
                
            def launch(self, *args, **kwargs) -> utils.MessageId:
                id_ = next(self.client.id_generator)
                self.client._logging('info', f'launch-task has been created ({id_})')
                self.client._new_thread(
                    target = self.client.push, 
                    thread_hint = self.launch.__name__, 
                    key = f'{id_}_{datetime.now()}', 
                    thread_kwargs = {
                        'data': utils.MessageConstructor.task(
                            # metadata
                            id_ = id_, 
                            client = self.client.name,
                            # arguments
                            lifetime = self.hard_task_lifetime if self.hard else self.task_lifetime,  
                            func = self.func, func_args = args, func_kwargs = kwargs,
                            delay = self.delay, hard = self.hard)}
                    )
                self.client._logging('info', f'launch-task has been created ({id_})')
                return id_
                            
        return Task(func, self)
           
    def push(self, data: Mapping, **kwargs) -> None:
        self.publisher.publish(data, **kwargs)
        self._logging('debug', f'A message ({data.get("id")}) has been sent to {self.publisher.name}')
        
    def get_response(self, message_id: utils.MessageId) -> dict | None:
        return self.consumer.get_response(message_id)
    
    def sync_await_responce(self, message_id: utils.MessageId) -> dict:
        while True:
            responce = self.get_response(message_id)
            if responce: 
                self._logging('info', f'response received (sync_await_response): {message_id}')
                return responce
    
    async def async_get_response(self, message_id: utils.MessageId, 
                                 heartbeat: float = 0.2) -> dict:
        while True:
            response = self.get_response(message_id)
            if response: 
                self._logging('info', f'response received (async_get_response): {message_id}')
                return response
            await asyncio.sleep(heartbeat) 