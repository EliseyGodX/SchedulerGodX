import enum
from datetime import datetime, timedelta
from functools import cached_property

from sqlalchemy import BLOB, Column, Enum, Boolean, String, create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session


class TaskStatus(enum.Enum):
    WAITING = 0
    WORK = 1
    COMPLETED = 2
    ERROR = 3
    CANCELLED = 4
    OVERDUE = 5


def servicemethod(method):
    def wrapper(self, *args, **kwargs):
        if not self.service_db:
            raise 'available only for service storage'
        return method(self, *args, **kwargs)
    return wrapper

class DB:
    TaskBase = declarative_base()
    ClientBase = declarative_base()
        
    class Task(TaskBase):
        __tablename__ = 'task'
        id = Column(String, primary_key=True)
        client = Column(String)
        status = Column(Enum(TaskStatus))
        time_to_start = Column(String)
        task = Column(BLOB)
        
    class Client(ClientBase):
        __tablename__ = 'client'
        name = Column(String, primary_key=True)
        enable_overdue = Column(Boolean)
        
    def __init__(self, path: str = 'sqlite:///SchedulerGodX.db', 
                 service_db: bool = False) -> None:
        self.service_db = service_db
        self.engine = create_engine(path)
        if not 'task' in inspect(self.engine).get_table_names():
            self.TaskBase.metadata.create_all(self.engine)
        if not 'client' in inspect(self.engine).get_table_names() and service_db:
            self.ClientBase.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    @servicemethod
    def add_client(self, client: dict) -> None:
        record = DB.Client(**client)
        self.session.add(record)
        self.session.commit()