from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from logging import Logger
from typing import Any, Generator, NoReturn

from schedulergodx.utils.id_generators import MessageId, ulid_generator
from schedulergodx.utils.logger import LoggerConstructor
from schedulergodx.utils.rmq_property import RmqConnect, rmq_default_settings


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