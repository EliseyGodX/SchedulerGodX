import logging
import logging.config
from logging import Logger, FileHandler, Formatter
from typing import Callable


class LoggerConstructor:
    
    def __init__(self, name: str = 'logger', log_file: str = 'schedulergodx.log', 
                 log_level: int = logging.INFO) -> None:
       self.logger = logging.getLogger(name)
       self.logger.setLevel(log_level)
       file_handler = FileHandler(log_file)
       file_handler.setLevel(log_level)
       formatter = Formatter('%(asctime)s - %(levelname)s - %(name)s:%(message)s')
       file_handler.setFormatter(formatter)       
       self.logger.addHandler(file_handler)
    
    @staticmethod
    def log_levels(logger: Logger) -> dict[str, Callable]:
        return {
            'debug': logger.debug,
            'info': logger.info,
            'error': logger.error,
            'critical': logger.critical,
            'fatal': logger.fatal
        }
        
    def addHandler(self, hdlr: logging.Handler) -> None:
       self.logger.addHandler(hdlr=hdlr)
    
    def addFilter(self, filter: logging.Filter) -> None:
        self.logger.addFilter(filter)
        
    def getLogger(self) -> Logger:
        return self.logger