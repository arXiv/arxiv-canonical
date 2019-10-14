"""Provides a :class:`.CanonicalDecoder` for domain objects."""

import json
from datetime import datetime, date
from enum import Enum
from typing import Any, Union, List, Dict, GenericMeta
from typing import TypingMeta  # type: ignore ; it's really there...
from uuid import UUID

from backports.datetime_fromisoformat import MonkeyPatch

from .. import domain


MonkeyPatch.patch_fromisoformat()


class CanonicalDecoder(json.JSONDecoder):
    """Decode domain objects."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Pass :func:`object_hook` to the base constructor."""
        kwargs['object_hook'] = kwargs.get('object_hook', self.object_hook)
        super(CanonicalDecoder, self).__init__(*args, **kwargs)

    def _try_isoparse(self, value: Any) -> Any:
        """Attempt to parse a value as an ISO8601 datetime."""
        if type(value) is not str:
            return value
        try:
            return date.fromisoformat(value)  # type: ignore ; pylint: disable=no-member
        except ValueError:
            try:
                return datetime.fromisoformat(value)  # type: ignore ; pylint: disable=no-member
            except ValueError:
                return value

    def object_hook(self, obj: dict, **extra: Any) -> Any:  # pylint: disable=method-hidden
        """Decode domain objects in this package."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if type(value) is list:
                    obj[key] = [self._try_isoparse(v) for v in value]
                else:
                    obj[key] = self._try_isoparse(value)

            # Look for and instantiate the domain class that corresponds to the
            # stated type of the data.
            obj_type = obj.pop('@type', None)
            if obj_type is None:
                return obj
            for domain_class in domain.domain_classes:
                if domain_class.__name__ == obj_type:
                    # Look for easy wins on casting field data to the correct
                    # type. The main use-case is for enums.
                    for field, ftype in domain_class.__annotations__.items():  # pylint: disable=protected-access
                        # These are things like Union, List, etc that don't
                        # have a concrete type. Too hard to take this on.
                        if isinstance(ftype, GenericMeta) \
                                or isinstance(type(ftype), TypingMeta):
                            continue
                        # Otherwise, this is a concrete type. We can try
                        # to cast here.
                        if field in obj \
                                and not isinstance(obj[field], ftype):
                            obj[field] = ftype(obj[field])
                    return domain_class(**obj)
        return obj


