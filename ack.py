from typing import Generic, TypeVar

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')

class Foo(Generic[T, U]):
    def __init__(self, baz: T, asdf: U) -> None:
        self.baz = baz
        self.asdf = asdf


class Bar(Foo[V, int]):
    ...


class Bat(Bar[str]):
    ...


x = Bat('foo', 1)
reveal_type(x)
reveal_type(x.baz)
reveal_type(x.asdf)