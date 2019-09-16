import json
from contextlib import contextmanager
from datetime import datetime
from pytz import UTC
from typing import Any, Deque, Dict, Iterator, Optional
from uuid import uuid4, UUID

import redis
from backports.datetime_fromisoformat import MonkeyPatch
from typing_extensions import Protocol

MonkeyPatch.patch_fromisoformat()


class IWriteLock(Protocol):
    name: str
    uuid: UUID

    def acquire_lock(self) -> None:
        ...

    def cold_start(self, timeout: int = 60) -> None:
        ...

    @contextmanager
    def holding_lock(self) -> Iterator[None]:
        ...


class Lock:
    def __init__(self, name: str, uuid: str, position: int,
                 timestamp: datetime) -> None:
        self.name = name
        self.uuid = uuid
        self.position = position
        self.timestamp = timestamp

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Lock':
        return cls(name=data['name'],
                   uuid=data['uuid'],
                   position=data['position'],
                   timestamp=datetime.utcfromtimestamp(data['timestamp']))

    def to_dict(self) -> Dict[str, Any]:
        return {'name': self.name,
                'uuid': self.uuid,
                'position': self.position,
                'timestamp': self.timestamp.timestamp()}


class ILockDeque:
    def append(self, lock: Lock) -> None:
        ...

    def appendleft(self, lock: Lock) -> None:
        ...

    def pop(self, timeout: Optional[int] = None) -> Lock:
        ...

    def popleft(self, timeout: Optional[int] = None) -> Lock:
        ...


class RedisLockDeque(ILockDeque):
    def __init__(self, client: redis.Redis, name: str) -> None:
        self._name = name
        self._client = client

    def append(self, lock: Lock) -> None:
        self._client.rpush(self._name, json.dumps(lock.to_dict()))

    def appendleft(self, lock: Lock) -> None:
        self._client.lpush(self._name, json.dumps(lock.to_dict()))

    def pop(self, timeout: Optional[int] = None) -> Lock:
        timeout = timeout if timeout is not None else 0
        return Lock.from_dict(json.loads(self._client.rpop(self._name, timeout))) # type: ignore

    def popleft(self, timeout: Optional[int] = None) -> Lock:
        timeout = timeout if timeout is not None else 0
        return Lock.from_dict(json.loads(self._client.lpop(self._name, timeout)))  # type: ignore


class WriteLock(IWriteLock):
    def __init__(self, name: str, queue: ILockDeque,
                 position: int = -1) -> None:
        self.name = name
        self._queue = queue
        self.uuid = uuid4()
        self.position = position

    def cold_start(self, timeout: int = 60) -> None:
        self._await_lock(timeout)
        self._send_lock()

    def acquire_lock(self) -> None:
        lock = self._await_lock()
        self.position = lock.position
        self._send_lock()

    @contextmanager
    def holding_lock(self) -> Iterator[None]:
        assert self._is_our_lock(self._await_lock())
        yield
        self._send_lock()

    def _is_our_lock(self, lock: Lock) -> bool:
        return bool(lock.position == self.position + 1
                    and lock.uuid == self.uuid.hex)

    def _send_lock(self) -> None:
        self._queue.append(Lock(name=self.name,
                                uuid=self.uuid.hex,
                                position=self.position + 1,
                                timestamp=datetime.now(UTC)))

    def _await_lock(self, timeout: Optional[int] = None) -> Lock:
        lock = self._queue.popleft(timeout)
        assert lock.name == self.name
        return lock
