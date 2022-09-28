import hashlib
from typing import Union, Callable, TypeVar, Iterator, Generic, Mapping

from .backend import Backend


T = TypeVar("T")
KT = TypeVar("KT")
VT_co = TypeVar('VT_co', covariant=True)
RegistryKey = Union[str, int]


class DeserializerMapping(Mapping[KT, VT_co]):
    """
    A wrapper for a mapping, which also deserializes the values upon access.

    In addition, values are cached when accessed.
    """
    def __init__(self, items: Mapping[KT, T], deserializer: Callable[[T], VT_co]):
        self.items = items
        self.deserializer = deserializer
        self._cache = {}

    def __getitem__(self, key: KT) -> VT_co:
        if key not in self._cache:
            self._cache[key] = self.deserializer(self.items[key])
        return self._cache[key]

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Iterator[KT]:
        return iter(self.items)


class SerializedItemsRegistry(Generic[T]):
    """
    A registry of serialized items, accessible from both the Workforce side and the workers.

    The registry is implemented using a Mapping appropriate for the backend used, and the serialization is also
    performed according to the backend.

    Parameters
    ----------
    backend : Backend
        The backend to use for communication.
    """
    def __init__(self, backend: Backend):
        self.backend = backend
        self.items = backend.dict()

    def register(self, item: T) -> RegistryKey:
        s_item = self.backend.serialize(item)
        key = self._make_key(s_item)
        if key not in self.items:
            self.items[key] = s_item
        return key

    @staticmethod
    def _make_key(serialized_item) -> RegistryKey:
        if isinstance(serialized_item, bytes):
            return hashlib.sha1(serialized_item).hexdigest()
        else:
            return id(serialized_item)

    def get_deserializer(self) -> DeserializerMapping[RegistryKey, T]:
        return DeserializerMapping(self.items, self.backend.deserialize)
