from typing import NamedTuple, Any, Generic, TypeVar, NamedTupleMeta, GenericMeta, Type, Dict, Iterable, Tuple, ClassVar
from operator import attrgetter
from typing_extensions import Protocol


T = TypeVar('T')
Self = TypeVar('Self', bound='Foo')

class Mixin:
    def what(self) -> None:
        print('!!')


class Foo(Generic[T], Mixin):
    a: T
    b: ClassVar[Type[T]]

    def __init__(self, a: T) -> None:
        self.a = a

    @classmethod
    def oof(cls: Type[Self], a: T) -> Self:
        return cls(a)


class Bar(Foo[int]):
    b = int

    @classmethod
    def baz(cls) -> None:
        # reveal_type(cls.a)
        pass



# reveal_type(Bar.b)
f = Bar.b(1)
reveal_type(f)
