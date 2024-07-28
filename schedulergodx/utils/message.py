import base64
import json
from collections import namedtuple
from datetime import timedelta
from enum import Enum
from typing import Callable, Iterable, Mapping, Optional, TypeAlias

import dill

from schedulergodx.utils.id_generators import MessageId

Serializable : TypeAlias = str | bytes | bytearray

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
    UNREGGISTERED_CLIENT = 2
    DUPLICAT_NAME = 3
    
    
class MessageConstructor:
    
    @staticmethod
    def func_serialization(func: Callable) -> str:
        return base64.b64encode(dill.dumps(func)).decode('utf-8')
    
    @staticmethod
    def initialization(id_: MessageId, client: str, **arguments) -> dict:
        return {
            'id': id_,
            'client': client,
            'type': Message.INITIALIZATION.value,
            'arguments': arguments
        }
    
    @staticmethod
    def info(id_: MessageId, client: str, **arguments) -> dict:
        return {
            'id': id_,
            'client': client,
            'type': Message.INFO.value,
            'arguments': arguments
        }
    
    @staticmethod
    def error(id_: MessageId, client: str, 
              error: MessageErrorStatus, message: str) -> dict:
        return {
            'id': id_,
            'client': client,
            'type': Message.ERROR.value,
            'arguments': {
                'error_code': error.value,
                'message': message
            }
        }
    
    @staticmethod
    def task(id_: MessageId, client: str, type: Message, lifetime: int, 
             func: Callable, func_args: Iterable, func_kwargs: Mapping, 
             delay: Optional[timedelta] = None, hard: bool = False) -> dict:
        return {
            'id': id_,
            'client': client,
            'type': type.value,
            'arguments': {
                'lifetime': lifetime,
                'function': MessageConstructor.func_serialization(func),
                'args': func_args,
                'kwargs': func_kwargs,
                'hard': hard,
                'delay': delay.total_seconds() if delay else None
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