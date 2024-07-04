from typing import Generator, TypeVar

from ulid import ulid

MessageId = TypeVar('MessageId', str, int)

def autoincrement(start_point: int = 0):
    current_id = start_point
    while True:
        current_id += 1
        yield current_id

def ulid_generator():
    while True:
        yield str(ulid())
        