import asyncio
import base64
import queue
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from logging import Logger
from typing import Any, Callable, Generator, Mapping, Sequence

import dill

import schedulergodx.utils as utils
from schedulergodx.client.consumer import Consumer
from schedulergodx.client.publisher import Publisher


@dataclass
class Client:
    name: str = 'client'
    log_name: str = 'core'
    responce_queue: queue.Queue = queue.Queue()
    thread_map: Mapping[str, threading.Thread] = field(default_factory=lambda: {
        'task_launch': {},
        'task_delayed_launch': {}
    })
    rmq_parameters: Mapping[str, Any] = field(default_factory=dict)
    rmq_credentials: Sequence[str] = field(default_factory=list)
    rmq_publisher_que: str = 'client-service'
    rmq_consumer_que: str = 'service-client'
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
    
    def _new_thread(self, target: Callable, type: str, 
                     key: str, *args, **kwargs) -> None:
        thread = threading.Thread(target=target, args=args, kwargs=kwargs)
        self.thread_map[type][key] = thread
        thread.start()
        self._logging('info', f'thread {key} ({type}) has started')
        
    def push(self, data: Mapping, **kwargs) -> None:
        self.publisher.publish(data, **kwargs)
        self._logging('debug', f'A message ({data.get("id")}) has been sent to {self.publisher.name}')
        
    def task(self, func: Callable):
        class Task:
            def __init__(self, func: Callable, client: Client):
                self.func = func
                self.client = client
                
            def launch(self, *args, **kwargs):
                id_ = next(self.client.id_generator)
                self.client._logging('info', f'launch-task has been created ({id_})')
                self.client._new_thread(target=self.client.push, type='task_launch', 
                                         key=str(id_), data={
                    'id': id_,
                    'client': self.client.name,
                    'type': utils.Message.TASK.value,
                    'arguments': {
                        'function': base64.b64encode(dill.dumps(self.func)).decode('utf-8'),
                        'args': args,
                        'kwargs': kwargs
                    }}
                )
                return id_
            
            def delayed_launch(self, *args, delay: timedelta, **kwargs):
                id_ = next(self.client.id_generator)
                self.client._new_thread(target=self.client.push, type='task_delayed_launch', 
                                         key=str(id_), data={
                    'id': id_,
                     'client': self.client.name,
                     'type': utils.Message.DELAYED_TASK.value,
                     'arguments': {
                        'delay': delay.total_seconds(),
                        'function': base64.b64encode(dill.dumps(self.func)).decode('utf-8'),
                        'args': args,
                        'kwargs': kwargs
                    }}
                )
                self.client._logging('info', f'delayed_launch-task has been created ({id_})')
                return id_
                                
        return Task(func, self)
           
    def get_response(self, message_id: utils.MessageId) -> Mapping | None:
        return self.consumer.get_response(message_id)
    
    async def async_get_response(self, message_id: utils.MessageId, heartbeat: int = 0.2) -> Mapping:
        while True:
            response = self.get_response(message_id)
            if response is not None: 
                self._logging('info', f'response received (async_get_response): {message_id}')
                return response
            await asyncio.sleep(heartbeat) 

    def get_threads(self, filter: Sequence[str] | None = None) -> Mapping:
        if filter is None: 
            return self.thread_map
        return {type_: self.thread_map[type_] for type_ in filter}
        
