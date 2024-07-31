import base64
import json
from collections import namedtuple
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Iterable, Mapping, Optional, TypeAlias

import dill

from schedulergodx.utils.id_generators import MessageId

Serializable : TypeAlias = str | bytes | bytearray
Seconds: TypeAlias = int

MessageDisassemble = namedtuple('MessageDisassemble', 'metadata arguments')

class Message(Enum):
    INITIALIZATION = 0
    INFO = 1
    ERROR = 2
    TASK = 3
    
    
class MessageInfoStatus(Enum):
    OK = 0    
    
    
class MessageErrorStatus(Enum):
    BAD_INITIALIZATION = 0
    INCORRECT_TYPE = 1
    UNREGISTERED_CLIENT = 2
    INVALID_TASK = 3
    ERROR_IN_TASK = 4
    TASK_TIMEOT_ERROR = 5
    
    
class MessageConstructor:
    
    @staticmethod
    def serialization(object: object) -> Serializable:
        return base64.b64encode(dill.dumps(object)).decode('utf-8')
    
    @staticmethod
    def deserialization(object: Serializable) -> object:
        return dill.loads(base64.b64decode(object))
    
    @staticmethod 
    def bulk_deserialization(*args: Iterable):
        return map(MessageConstructor.deserialization, args)
    
    @staticmethod
    def initialization(id: MessageId, client: str, **arguments) -> dict:
        return {
            'id': id,
            'client': client,
            'type': Message.INITIALIZATION.value,
            'arguments': arguments
        }
    
    @staticmethod
    def info(id: MessageId, client: str, **arguments) -> dict:
        return {
            'id': id,
            'client': client,
            'type': Message.INFO.value,
            'arguments': arguments
        }
    
    @staticmethod
    def error(id: MessageId, client: str, 
              error: MessageErrorStatus, message: str) -> dict:
        return {
            'id': id,
            'client': client,
            'type': Message.ERROR.value,
            'arguments': {
                'error_code': error.value,
                'message': message
            }
        }
    
    @staticmethod
    def task(id: MessageId, client: str, lifetime: int, 
            func: Callable, func_args: Iterable, func_kwargs: Mapping, 
            delay: Optional[Seconds] = None, hard: bool = False) -> dict: 
        if delay:
            time_to_start = timedelta(seconds=delay) + datetime.now()
        else: 
            time_to_start = datetime.now()
        return {
            'id': id,
            'client': client,
            'type': Message.TASK.value,
            'arguments': {
                'lifetime': lifetime,
                'function': MessageConstructor.serialization(func),
                'args': MessageConstructor.serialization(func_args),
                'kwargs': MessageConstructor.serialization(func_kwargs),
                'time_to_start': MessageConstructor.serialization(time_to_start),
                'hard': hard
            }
        }
    
    @staticmethod
    def disassemble(message: dict | Serializable) -> MessageDisassemble:
        try:
            body = json.loads(message)
        except TypeError:
            body = message
        metadata = {
            'id': body['id'],
            'client': body['client'],
            'type': Message(body['type'])
        }
        arguments = body['arguments']
        return MessageDisassemble(metadata, arguments)