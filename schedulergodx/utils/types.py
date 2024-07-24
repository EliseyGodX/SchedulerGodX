from enum import Enum
from typing import Any


class Message(Enum):
    INFO = 0
    TASK = 1
    DELAYED_TASK = 2
    
    
class Error(Enum):
    IncorrectType = 'the received message has an incorrect type'
    
    
class AllClients:
    
    def __eq__(self, value: object) -> bool:
        return True