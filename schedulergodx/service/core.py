import concurrent.futures
import json
import multiprocessing
import threading
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from logging import Logger
from typing import NoReturn

from sqlalchemy.orm.session import Session

import schedulergodx.utils as utils
from schedulergodx.service.consumer import Consumer
from schedulergodx.service.publisher import Publisher
from schedulergodx.utils.logger import LoggerConstructor
from schedulergodx.utils.storage import DB


class _Client:
    
    def __init__(self, name: str, enable_overdue: bool = False) -> None:
        self.name = name
        self.enable_overdue = enable_overdue
    
    def __repr__(self) -> str:
        return self.name
        
    def __eq__(self, other_client: '_Client') -> bool:
        return self.name == other_client
    
    
class Task:
    
    def __init__(self, id: utils.MessageId, time_to_start: utils.Serializable, db: DB) -> None:
        self.db = db
        self.id = id
        self.time_to_start: datetime = utils.MessageConstructor.deserialization(time_to_start)
        self.overdue = False if self.get_time_delta() > 0 else True
        
    def __repr__(self) -> str:
        return self.id
    
    def get_time_delta(self) -> utils.Seconds:
        return int(
            (self.time_to_start - datetime.now())
            .total_seconds()
            )
    
    def get_delay(self) -> utils.Seconds:
        delay = self.get_time_delta()
        if delay <= 0: return 0
        return delay
        
    def db_save(self, db_session: Session, client: str, func: utils.Serializable, func_args: utils.Serializable, 
                func_kwargs: utils.Serializable, lifetime: int, hard: bool = False) -> None:
        task = self.db.Task(
           id = self.id,
           client = client,
           status = utils.TaskStatus.WAITING,
           time_to_start = utils.MessageConstructor.serialization(self.time_to_start),
           task = func,
           task_args = func_args,
           task_kwargs = func_kwargs,
           lifetime = lifetime,
           hard = hard
        )
        db_session.add(task)
        db_session.commit()
        
    def run(self, db_session: Session) -> utils.DB.Task:
        task: utils.DB.Task = db_session.query(self.db.Task).get(self.id)
        task.status = utils.TaskStatus.WORK
        db_session.commit()
        return task
        

class ClientPool:
    
    def __init__(self, db: utils.DB, clients: list = []) -> None:
        self.clients: list[_Client] = clients
        self.db = db
    
    def __repr__(self) -> str:
        return f'<ClientPool (size: {len(self.clients)})>'
        
    def __contains__(self, item: _Client) -> bool:
        return item in self.clients
    
    def append(self, client: _Client, db_session: Session) -> None:
        self.clients.append(client)
        self.db.add_client(client.__dict__, db_session)
        
    def get_client_by_name(self, name: str) -> _Client| None:
        for client in self.clients:
            if name == client.name:
                return client
                
    
@dataclass
class Service(utils.AbstractionCore):
    name: str = 'service'
    db: DB = DB(service_db=True)
    
    def __post_init__(self) -> None:
        self.publisher = Publisher('publisher', rmq_que=self.rmq_publisher_que, 
                                   logger=self.logger, rmq_connect=self.rmq_connect)
        self.consumer = Consumer('consumer', rmq_que=self.rmq_consumer_que, 
                                 logger=self.logger, rmq_connect=self.rmq_connect)
        self.db_session = self.db.get_session()
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
   
    def _pre_start(self) -> None:
        clients = [_Client(**client) for client in self.db.get_clients_dicts(self.db_session)]
        self.client_pool = ClientPool(self.db, clients)
        self._launch_unfulfilled_tasks()
        self._logging('info', 'pre-start successful')
   
    def _error_message(self, message_id: utils.MessageId, client: str, 
                       error: utils.MessageErrorStatus, error_message: str) -> None:
        self.publisher.publish(data=utils.MessageConstructor.error(
            id = message_id, client = client,
            error = error, message = error_message
        ))
        self._logging('error', f'Error {error} (message id: {message_id}, client: {client})')
        
    def _launch_unfulfilled_tasks(self) -> None:
        for db_task in self.db.get_unfulfilled_tasks(self.db_session):
            task_client = self.client_pool.get_client_by_name(db_task.client)
            task = Task(
                id = db_task.id,
                time_to_start = db_task.time_to_start,
                db = self.db
                )
            if not task_client: 
                db_task.status = utils.TaskStatus.ORPHAN
                self.db_session.commit()
            elif (not task.overdue) or (task.overdue and task_client.enable_overdue):
                self._add_task(
                    task = task,
                    hard = db_task.hard
                    )
        
    def _task_work(self, task: Task) -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:   
            thread_db_session = self.db.get_session()                        
            task = task.run(thread_db_session)
            func, args, kwargs = utils.MessageConstructor.bulk_deserialization(
                task.task, task.task_args, task.task_kwargs
                )
            future = executor.submit(func, *args, **kwargs)
            try:
                future.result(timeout=task.lifetime)
                self._logging('info', f'task is completed (id: {task.id})')
                self.publisher.publish(utils.MessageConstructor.info(
                    id = task.id, client = task.client,
                    responce = utils.MessageInfoStatus.OK.value
                ))
                task.status = utils.TaskStatus.COMPLETED
            except concurrent.futures.TimeoutError:
                task.status = utils.TaskStatus.ERROR
                self._error_message(message_id = task.id, client = task.client, 
                                    error = utils.MessageErrorStatus.TASK_TIMEOT_ERROR,
                                    error_message = f'task {task.id} was canceled due to an error timeout')            
            except Exception as e:
                task.status = utils.TaskStatus.ERROR
                self._error_message(message_id = task.id, client = task.client, 
                                    error = utils.MessageErrorStatus.ERROR_IN_TASK,
                                    error_message = f'task {task.id}: {e}')
            finally:
                thread_db_session.commit()
                
    def _hard_task_work(self, task: Task) -> None:
        thread_db_session = self.db.get_session()
        task = task.run(thread_db_session)
        func, args, kwargs = utils.MessageConstructor.bulk_deserialization(
                task.task, task.task_args, task.task_kwargs
                )
        process = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
        try:
            process.start()
            process.join(float(task.lifetime))
            if process.is_alive():
                process.terminate()
                process.join()
                task.status = utils.TaskStatus.ERROR
                self._error_message(
                    message_id = task.id, client = task.client,
                    error = utils.MessageErrorStatus.TASK_TIMEOT_ERROR,
                    error_message = f'task {task.id} was canceled due to an error timeout'
                )
        except Exception as e:
            task.status = utils.TaskStatus.ERROR
            self._error_message(message_id = task.id, client = task.client, 
                                error = utils.MessageErrorStatus.ERROR_IN_TASK,
                                error_message = f'task {task.id}: {e}')
        finally:
            thread_db_session.commit()
            
    def _add_task(self, task: Task, hard: bool = False) -> None:
        def wrapper() -> None:
            self._task_work(task)
        def hard_wrapper() -> None:
            self._hard_task_work(task)
        timer = threading.Timer(
            interval = task.get_delay(),
            function = wrapper if not hard else hard_wrapper
            )
        timer.start()
        
    def _on_message(self, channel, method_frame, header_frame, body) -> None:
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
                self.client_pool.append(client, self.db_session)
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
                        db_session = self.db_session,
                        client = message.metadata['client'],
                        func = message.arguments['function'],
                        func_args = message.arguments['args'],
                        func_kwargs = message.arguments['kwargs'],
                        lifetime = message.arguments['lifetime'],
                        hard = message.arguments['hard']
                    )
                    self._add_task(task, hard=message.arguments['hard'])
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
        
    def start(self) -> NoReturn:         
        self._pre_start()       
        self.consumer.start_consuming(self._on_message)
        
        
