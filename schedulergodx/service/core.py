
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, List, Mapping

from schedulergodx.utils.logger import LoggerConstructor
import pika


@dataclass
class Service:
    name: str = 'service'
    active_client: List[str] = field(default_factory=list)
    rmq_parameters: Mapping[str, Any] = field(default_factory=dict)
    logger: Logger = LoggerConstructor(name='serviceLog').getLogger()
