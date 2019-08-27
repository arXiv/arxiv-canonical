"""Provides a :class:`.CanonicalDecoder` for domain objects."""

import json
from typing import Any, Union, List, Dict, GenericMeta, TypingMeta
from datetime import datetime, date
from enum import Enum

from backports.datetime_fromisoformat import MonkeyPatch

from . import classic
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
            return date.fromisoformat(value)   # type: ignore
        except ValueError:
            try:
                return datetime.fromisoformat(value)  # type: ignore
            except ValueError:
                return value

    def object_hook(self, obj: dict, **extra: Any) -> Any:
        """Decode domain objects in this package."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if type(value) is list:
                    obj[key] = [self._try_isoparse(v) for v in value]
                else:
                    obj[key] = self._try_isoparse(value)

            # Look for and instantiate the domain class that corresponds to the
            # stated type of the data.
            obj_type = obj.pop('@type')
            for domain_class in domain.domain_classes:
                if domain_class.__name__ == obj_type:
                    # Look for easy wins on casting field data to the correct
                    # type. The main use-case is for enums.
                    for field, ftype in domain_class._field_types.items():
                        if isinstance(ftype, GenericMeta) \
                                or isinstance(type(ftype), TypingMeta):
                            # These are things like Union, List, etc that don't
                            # have a concrete type. Too hard to take this on.
                            continue
                        if field in obj and not isinstance(obj[field], ftype):
                            obj[field] = ftype(obj[field])
                    return domain_class(**obj)
        return obj


