import bisect
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property
from logging import Logger
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
    
    def __init__(self, db: DB, id: utils.MessageId,
                 time_to_start: utils.Serializable) -> None:
        self.db = db
        self.id = id
        self.time_to_start: datetime = utils.MessageConstructor.deserialization(time_to_start)
        
    def __repr__(self) -> str:
        return self.id
    
    def __lt__(self, other_task: 'Task') -> bool:
        return self.get_delay() < other_task.get_delay()
    
    def get_delay(self) -> int:
        return int(
            (self.time_to_start - datetime.now())
            .total_seconds()
            )
        
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
            task.client, task.lifetime, task.hard,
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
        

class TaskPool:
    
    def __init__(self) -> None:
        self.tasks: list[Task] = []
        
    def __repr__(self) -> str:
        return f'<TaskPool (size: {len(self.tasks)})>'
    
    def append(self, task: Task) -> None:
        bisect.insort(self.tasks, task)
        
    def delay(self) -> int:
        return self.tasks[0].get_delay()
        
    def task(self) -> Task:
        return self.tasks.pop(0)
                
    
@dataclass
class Service(utils.AbstractionCore):
    name: str = 'service'
    db: DB = DB(path='sqlite://SchedulerGodX_Service', service_db=True)
    
    def __post_init__(self) -> None:
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, 
                                   logger=self.logger, rmq_connect=self.rmq_connect)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, 
                                 logger=self.logger, rmq_connect=self.rmq_connect)
        self.client_pool: ClientPool = ClientPool(db=self.db)
        self.task_pool: TaskPool = TaskPool()
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
                    self.client_pool.append(_Client)
                    self._logging('info', f'client {client} has been initialized') 
                    self.publisher.publish(data = utils.MessageConstructor.info(
                        id_ = message.metadata['id'],
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
                            lifetime = message.metadata['lifetime']
                        )
                        self.task_pool.append(task)
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
        
