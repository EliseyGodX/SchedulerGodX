import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from logging import Logger
from typing import (Any, Callable, Iterable, Mapping, MutableMapping, Optional,
                    TypeAlias)

import schedulergodx.utils as utils
from schedulergodx.client.consumer import Consumer
from schedulergodx.client.publisher import Publisher
from schedulergodx.utils.logger import LoggerConstructor

ThreadMap: TypeAlias = (
    MutableMapping[
        str, threading.Thread
            ]
)

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
        self._thread_map: ThreadMap = {}
        id_ = next(self.id_generator)
        self.push(data=utils.MessageConstructor.initialization(
            id = id_, client = self.name,
            enable_overdue = self.enable_overdue
        ))
        responce = self.sync_await_responce(id_)
        if responce.metadata['type'] == utils.Message.ERROR:
            error = f'{responce.arguments["error_code"]} - {responce.arguments["message"]}'
            self._logging('fatal', error)
            raise Exception(error)
        elif responce.arguments['responce'] == utils.MessageInfoStatus.OK.value:
            self._logging('info', 'successful initialization')
        else: 
            raise Exception(responce)
        
    @property
    def rmq_publisher_que(self) -> str:
        return 'client-service'
    
    @property
    def rmq_consumer_que(self) -> str:
       return 'service-client'
   
    @cached_property
    def logger(self) -> Logger:
       return LoggerConstructor(name=self.name).getLogger()
    
    def _new_thread(self, target: Callable, key: str, thread_hint: Optional[str] = None, 
                    thread_args: Iterable[Any] = (), thread_kwargs: Mapping[str, Any] | None = None) -> None:
        thread = threading.Thread(target=target, args=thread_args, kwargs=thread_kwargs)
        self._thread_map[key] = thread
        thread.start()
        self._logging('info', f'thread {key} {("("+thread_hint+")") if thread_hint else ""} has started')
        
    def get_threads(self) -> ThreadMap:
        return self._thread_map
    
    def get_thread(self, id: str) -> threading.Thread | None:
        threads = self._thread_map.keys()
        for thread in threads:
            if f'{id}_' in thread:
                return self._thread_map[thread]
    
    def task(self, func: Callable):
        class Task:
            def __init__(self, func: Callable, client: Client, 
                         delay: Optional[utils.Seconds] = None, hard: bool = False) -> None:
                self._func = func
                self._client = client
                self.task_lifetime = client.task_lifetime
                self.hard_task_lifetime = client.hard_task_lifetime
                self.delay = delay
                self.hard = hard
                
            def set_parameters(self, **kwargs) -> None:
                self.__dict__.update(kwargs)
                
            def launch(self, *args, **kwargs) -> utils.MessageId:
                id_ = next(self._client.id_generator)
                self._client._logging('info', f'launch-task has been created ({id_})')
                self._client._new_thread(
                    target = self._client.push, 
                    thread_hint = self.launch.__name__, 
                    key = f'{id_}_{datetime.now()}', 
                    thread_kwargs = {
                        'data': utils.MessageConstructor.task(
                            id = id_, client = self._client.name,
                            lifetime = self.hard_task_lifetime if self.hard else self.task_lifetime,  
                            func = self._func, func_args = args, func_kwargs = kwargs,
                            delay = self.delay, hard = self.hard)}
                    )
                self._client._logging('info', f'launch-task has been created ({id_})')
                return id_
                            
        return Task(func, self)
           
    def push(self, data: Mapping, **kwargs) -> None:
        self.publisher.publish(data, **kwargs)
        self._logging('debug', f'A message ({data.get("id")}) has been sent to {self.publisher.name}')
        
    def get_response(self, message_id: utils.MessageId) -> utils.MessageDisassemble | None:
        return self.consumer.get_response(message_id)
    
    def sync_await_responce(self, message_id: utils.MessageId) -> utils.MessageDisassemble:
        while True:
            responce = self.get_response(message_id)
            if responce: 
                self._logging('info', f'response received (sync_await_response): {message_id}')
                return responce
    
    async def async_get_response(self, message_id: utils.MessageId, 
                                 heartbeat: float = 0.2) -> utils.MessageDisassemble:
        while True:
            response = self.get_response(message_id)
            if response: 
                self._logging('info', f'response received (async_get_response): {message_id}')
                return response
            await asyncio.sleep(heartbeat) 