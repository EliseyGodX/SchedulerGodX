import bisect
import concurrent.futures
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property
from logging import Logger
import threading
from typing import (Any, Callable, Generator, Iterable, Mapping,
                    MutableSequence, NoReturn, Optional)

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
        
    def __eq__(self, other_client: '_Client') -> bool:
        return self.name == other_client
    
    
class Task:
    
    def __init__(self, id: utils.MessageId, time_to_start: utils.Serializable, 
                 db: Optional[DB] = None) -> None:
        self.db = db
        self.id = id
        self.time_to_start: datetime = utils.MessageConstructor.deserialization(time_to_start)
        
    def __repr__(self) -> str:
        return self.id
    
    def get_delay(self) -> int:
        delay =  int(
            (self.time_to_start - datetime.now())
            .total_seconds()
            )
        if delay <= 0: return 0
        return delay
        
    def db_save(self, client: str, func: utils.Serializable, func_args: utils.Serializable, 
                func_kwargs: utils.Serializable, lifetime: int) -> None:
        task = self.db.Task(
           id = self.id,
           client = client,
           status = utils.TaskStatus.WAITING,
           time_to_start = utils.MessageConstructor.serialization(self.time_to_start),
           task = func,
           task_args = func_args,
           task_kwargs = func_kwargs,
           lifetime = lifetime
        )
        self.db.session.add(task)
        self.db.session.commit()
        
    def run(self) -> tuple:
        task = self.db.session.query(self.db.Task).get(self.id)
        task.status = utils.TaskStatus.WORK
        self.db.session.commit()
        return (
            task.client, task.lifetime,
            *utils.MessageConstructor.bulk_deserialization(
                task.task, task.task_args, task.task_kwargs
                )
        )
        

class ClientPool:
    
    def __init__(self, db: utils.DB) -> None:
        self.clients: list[_Client] = []
        self.db = db
    
    def __repr__(self) -> str:
        return f'<ClientPool (size: {len(self.clients)})>'
        
    def append(self, client: _Client) -> None:
        self.clients.append(client)
        self.db.add_client(client.__dict__)
        
    def __contains__(self, item: _Client) -> bool:
        return item in self.clients
                
    
@dataclass
class Service(utils.AbstractionCore):
    name: str = 'service'
    db: DB = DB(service_db=True)
    
    def __post_init__(self) -> None:
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, 
                                   logger=self.logger, rmq_connect=self.rmq_connect)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, 
                                 logger=self.logger, rmq_connect=self.rmq_connect)
        self.client_pool: ClientPool = ClientPool(db=self.db)
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
            id = message_id, client = client,
            error = error, message = error_message
        ))
        self._logging('error', f'Error {error} (message id: {message_id}, client: {client})')
        
    def _task_work(self, task: Task) -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            client, lifetime, func, args, kwargs = task.run()
            future = executor.submit(func, *args, **kwargs)
            try:
                future.result(timeout=lifetime)
                self._logging('info', f'task is completed (id: {task.id})')
                self.publisher.publish(utils.MessageConstructor.info(
                    id = task.id, client = client,
                    responce = utils.MessageInfoStatus.OK.value
                ))
            except concurrent.futures.TimeoutError:
                self._error_message(id = task.id, client = client, 
                                    error = utils.MessageErrorStatus.TASK_TIMEOT_ERROR,
                                    error_message = f'task {task.id} was canceled due to an error timeout')            
            except Exception as e:
                self._error_message(message_id = task.id, client = client, 
                                    error = utils.MessageErrorStatus.ERROR_IN_TASK,
                                    error_message = f'task {task.id}: {e}')
            
    def _add_task(self, task: Task) -> None:
        def wrapper() -> None:
            self._task_work(task)
        timer = threading.Timer(
            interval = task.get_delay(),
            function = wrapper
            )
        timer.start()                
        
    def _start_consuming(self) -> NoReturn:
        def on_message(channel, method_frame, header_frame, body) -> None:
            channel.basic_ack(method_frame.delivery_tag)
            try:
                message = utils.MessageConstructor.disassemble(body)
            except json.JSONDecodeError:
                return self._logging('error', 'a non-json message was received')
            except KeyError:
                return self._logging('error', 'the received message has an incorrect format')
                
            if (message.metadata['client'] not in self.client_pool 
                and message.metadata['type'] != utils.Message.INITIALIZATION):
                return self._error_message(
                    message_id = message.metadata['id'], 
                    client = message.metadata['client'],
                    error = utils.MessageErrorStatus.UNREGISTERED_CLIENT,
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
                    self.client_pool.append(client)
                    self._logging('info', f'client {client} has been initialized') 
                    self.publisher.publish(data = utils.MessageConstructor.info(
                        id = message.metadata['id'],
                        client = message.metadata['client'],
                        responce = utils.MessageInfoStatus.OK.value
                    ))
                
                case utils.Message.INFO:
                    return self._logging('info', f'info message: {body}')
                
                case utils.Message.TASK: 
                    try:
                        task = Task(
                            db = self.db,
                            id = message.metadata['id'],
                            time_to_start = message.arguments['time_to_start'] 
                        )
                        task.db_save(
                            client = message.metadata['client'],
                            func = message.arguments['function'],
                            func_args = message.arguments['args'],
                            func_kwargs = message.arguments['kwargs'],
                            lifetime = message.arguments['lifetime']
                        )
                        self._add_task(task)
                        self._logging('info', f'The task was received (id: {task.id})')
                    except:
                        return self._error_message(
                            message_id = message.metadata['id'],
                            client = message.metadata['client'],
                            error = utils.MessageErrorStatus.INVALID_TASK,
                            error_message = 'the task has an incorrect format'
                        )
                
                case _:
                     return self._error_message(
                        message_id = message.metadata['id'],
                        client = message.metadata['client'],
                        error = utils.MessageErrorStatus.INCORRECT_TYPE,
                        error_message = 'invalid message type received'
                    )
                    
        self.consumer.start_consuming(on_message)
        
        
