"""Provides the write log for the canonical record."""
import json
import os
from datetime import datetime
from typing import Iterable, Optional

from backports.datetime_fromisoformat import MonkeyPatch
from pytz import timezone

from .. import domain as D

MonkeyPatch.patch_fromisoformat()

Action = str
Outcome = str

SUCCEEDED: Outcome = 'SUCCESS'
FAILED: Outcome = 'FAILED'

DEREFERENCE: Action = 'DEREF'
READ: Action = 'READ'
WRITE: Action = 'WRITE'

ET = timezone('US/Eastern')


class LogEntry:
    timestamp: datetime
    """The time of the log entry."""

    event_id: D.EventIdentifier
    """Identifier of the event being handled."""

    # key: D.Key
    # """Specific key being handled."""

    action: Action
    """Action being performed by the agent."""

    state: Outcome
    """Outcome of the action."""

    message: str
    """Additional unstructured information about the action."""

    def __init__(self, timestamp: datetime,
                 event_id: D.EventIdentifier,
                #  key: D.Key,
                 action: Action,
                 state: Outcome,
                 message: str) -> None:
        self.timestamp = timestamp
        self.event_id = event_id
        self.action = action
        self.state = state
        self.message = message

    @classmethod
    def from_repr(cls, repr: str) -> 'LogEntry':
        data = json.loads(repr)
        return cls(
            timestamp=datetime.fromisoformat(data['timestamp']),  # type: ignore ; pylint: disable=no-member
            event_id=D.EventIdentifier(data['event_id']),
            # key=D.Key(data['key']),
            action=data['action'],
            state=data['state'],
            message=data.get('message', '')
        )

    def __repr__(self) -> str:
        return json.dumps({
            'timestamp': self.timestamp.isoformat(),
            'event_id': self.event_id,
            'action': self.action,
            'state': self.state,
            'message': self.message
        })


class Log:
    """Action log for a canonical agent."""

    def __init__(self, path: str) -> None:
        """Initialize with a reader and writer."""
        self.path = os.path.abspath(path)
        if not os.path.exists(self.path):
            raise RuntimeError(f'No such path: {self.path}')
        try:
            self._writer = open(self.current_log_path, 'a')
            self._reader = open(self.current_log_path, 'r')
        except Exception as e:
            raise RuntimeError(f'Could not open {self.path} for writing')

    @property
    def current_log_path(self) -> str:
        """The path to the current log file."""
        return f'{self.path}/.{datetime.now(ET).date().isoformat()}.log'

    def write(self,
              event_id: D.EventIdentifier,
              action: Action,
              state: Outcome,
              message: str) -> LogEntry:
        """Write a log entry."""
        entry = LogEntry(datetime.now(ET),
                         event_id,
                         action,
                         state,
                         message)
        self._writer.write(f'{entry}\n')
        self._writer.flush()    # So that the reader can see what's up.
        return entry

    def log_success(self,
                    event_id: D.EventIdentifier,
                    # key: D.Key,
                    action: Action,
                    message: str = '') -> LogEntry:
        """Log a successful action."""
        return self.write(event_id, action, SUCCEEDED, message)

    def log_failure(self,
                    event_id: D.EventIdentifier,
                    # key: D.Key,
                    action: Action,
                    message: str = '') -> LogEntry:
        """Log a failed action."""
        return self.write(event_id, action, FAILED, message)

    def read_last_entry(self) -> LogEntry:
        """Read the last entry in the log."""
        entry = LogEntry.from_repr(self._reader.readlines()[-1])
        self._reader.seek(0)
        return entry

    def read_last_succeeded(self) -> Optional[LogEntry]:
        """Read the last SUCCEEDED entry in the log."""
        lines = self._reader.readlines()
        for i in range(1, len(lines)):
            entry = LogEntry.from_repr(lines[-i])
            if entry.state == SUCCEEDED:
                return entry
        return None

    def read_all(self) -> Iterable[LogEntry]:
        for line in self._reader.readlines():
            yield LogEntry.from_repr(line)
