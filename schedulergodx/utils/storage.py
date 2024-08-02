import enum
from typing import Any, List

from sqlalchemy import (Boolean, Column, Enum, Integer, String, create_engine,
                        inspect, or_)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.session import Session


class TaskStatus(enum.Enum):
    WAITING = 0
    WORK = 1
    COMPLETED = 2
    ERROR = 3
    CANCELLED = 4
    OVERDUE = 5
    ORPHAN = 6


def servicemethod(method):
    def wrapper(self, *args, **kwargs):
        if not self.service_db:
            raise Exception('available only for service storage')
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
        task = Column(String)
        task_args = Column(String, nullable=True)
        task_kwargs = Column(String, nullable=True)
        lifetime = Column(Integer)
        hard = Column(Boolean)
        
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
        session_factory = sessionmaker(bind=self.engine)
        self._Session = scoped_session(session_factory)
    
    def get_session(self) -> Session:
        return self._Session()

    def get_unfulfilled_tasks(self, session: Session) -> List[Task]:
        return (
            session.query(DB.Task)
            .filter(
                or_(
                    DB.Task.status == TaskStatus.WAITING,
                    DB.Task.status == TaskStatus.WORK
                    )
            )
            .all()
        )
    
    @servicemethod
    def add_client(self, client: dict, session: Session) -> None:
        client = DB.Client(**client)
        session.merge(client)
        session.commit()
        
    @servicemethod
    def get_clients(self, session: Session) -> list[Client]:
        return session.query(DB.Client).all()
    
    @servicemethod
    def get_clients_dicts(self, session: Session) -> list[dict[str, Any]]:
        clients = []
        for client in self.get_clients(session):
            clients.append(
                {c.key: getattr(client, c.key) for c 
                 in inspect(client).mapper.column_attrs}
                )
        return clients