from typing import Any, Callable, Dict, Iterable, Type, TypeVar, Union, cast
from typing_extensions import Protocol, runtime_checkable


class CanonicalBase:
    """Base class for all canonical domain classes."""

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CanonicalBase):
            return False
        keys = (set(self.__class__.__annotations__.keys())  # pylint: disable=no-member ; subclasses have annotations.
                | set(other.__class__.__annotations__.keys()))
        try:
            for key in keys:
                assert getattr(self, key) == getattr(other, key)
        except AssertionError:
            return False
        return True


class CanonicalBaseCollection(CanonicalBase):
    """Base class for domain classes that act as collections."""


_O = TypeVar('_O', bound=CanonicalBase)
_T = TypeVar('_T', bound=Union[CanonicalBase, Dict[str, Any]])

Callback = Callable[[_O, _T], _T]


def iter_apply(funcs: Iterable[Callback], obj: _O, data: _T) -> _T:
    for func in funcs:
        data = func(obj, data)
    return data


def filter_callbacks(funcs: Iterable[Callback], klass: Type) \
        -> Iterable[Callback]:
    return filter(lambda f: f.__annotations__['obj'] is klass, funcs)


FuncType = Callable[..., Any]
F = TypeVar('F', bound=FuncType)


def with_callbacks(func: F) -> F:
    def wrapper(obj: Any, *args: Any, callbacks: Iterable[Callback] = (),
                **kwargs: Any) -> Any:
        T = obj if isinstance(obj, type) else type(obj)
        return iter_apply(filter_callbacks(callbacks, T), obj,
                          func(obj, *args, callbacks=callbacks, **kwargs))
    return cast(F, wrapper)
