from typing import Generator, TypeAlias, NoReturn, Optional

from ulid import ulid

MessageId: TypeAlias = str

def autoincrement(start_point: int = 0) -> Generator[MessageId, Optional[int], NoReturn]:
    current_id = start_point
    while True:
        current_id += 1
        yield str(current_id)

def ulid_generator() -> Generator[MessageId, None, NoReturn]:
    while True:
        yield str(ulid())
        