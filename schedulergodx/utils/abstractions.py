from functools import cached_property
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta
from logging import Logger
from typing import (Any, Callable, Generator, Optional, MutableMapping,
                    NoReturn, Iterable, TypeAlias, Mapping)

from schedulergodx.utils.logger import LoggerConstructor
from schedulergodx.utils.id_generators import MessageId, ulid_generator
from schedulergodx.utils.rmq_property import RmqConnect, rmq_default_settings

ThreadMap: TypeAlias = (
    MutableMapping[
        str, threading.Thread
            ]
)

class AbstractionConnectClass(ABC):
    
    def __init__(self, name: str, *, rmq_que: str, logger: Logger, rmq_connect: RmqConnect) -> None:
        self.name = name
        self.logger = logger
        self.queue = rmq_que
        self.channel = rmq_connect.get_channel(rmq_que)
        
    def _logging(self, level: str, message: str) -> None:
        LoggerConstructor.log_levels(self.logger)[level](f'{self.name} - {message}')
        

@dataclass
class AbstractionCore(ABC):    
    core_name: str = 'core'
    _thread_map: ThreadMap = field(default_factory=lambda: {})
    rmq_connect: RmqConnect = RmqConnect(
        rmq_parameters=rmq_default_settings.parametrs,
        rmq_credentials=rmq_default_settings.credentials
    )
    id_generator: Generator[MessageId, Any, NoReturn] = ulid_generator()
    
    @property
    @abstractmethod
    def rmq_publisher_que(self) -> str:
        ''' '''
        
    @property
    @abstractmethod
    def rmq_consumer_que(self) -> str:
        ''' '''
        
    @cached_property
    @abstractmethod
    def logger(self) -> Logger:
        ''' '''
    
    def _logging(self, level: str, message: str) -> None:
        LoggerConstructor.log_levels(self.logger)[level](f'{self.core_name} - {message}')
    
    def _new_thread(self, target: Callable, key: str, thread_hint: Optional[str] = None, 
                    thread_args: Iterable[Any] = (), thread_kwargs: Mapping[str, Any] | None = None) -> None:
        thread = threading.Thread(target=target, args=thread_args, kwargs=thread_kwargs)
        self._thread_map[key] = thread
        thread.start()
        self._logging('info', f'thread {key} {("("+thread_hint+")") if thread_hint else ""} has started')
        
    def get_threads(self) -> ThreadMap:
        return self._thread_map
    
    def get_thread(self, id_: str) -> threading.Thread:
        threads = self._thread_map.keys()
        for thread in threads:
            if f'{id_}_' in thread:
                return self._thread_map[thread]