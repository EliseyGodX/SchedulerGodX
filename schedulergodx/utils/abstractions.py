import json
from abc import ABC
from logging import Logger
from typing import Any, Mapping, NoReturn, Sequence

import pika

from schedulergodx.utils.logger import LoggerConstructor
from schedulergodx.utils.rmqProperty import RmqConnect


class AbstractionConnectClass(ABC):
    
    def __init__(self, name: str, *, rmq_que: str, logger: Logger, rmq_connect: RmqConnect) -> None:
        self.name = name
        self.logger = logger
        self.queue = rmq_que
        self.channel = rmq_connect.get_channel(rmq_que)
        
    def _logging(self, level: str, message: str) -> None:
        LoggerConstructor.log_levels(self.logger)[level](f'{self.name} - {message}')