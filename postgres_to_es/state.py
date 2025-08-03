import abc
import json
from typing import Any, Dict
from redis import Redis

class BaseStorage(abc.ABC):
    """
    Абстрактное хранилище состояния.
    Позволяет сохранять и получать состояние. Способ хранения может варьироваться:
    база данных, файловое хранилище и т.д.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""

class RedisStorage(BaseStorage):
    """
    Хранилище состояния, использующее Redis.
    Данные хранятся в виде JSON-строки по заданному ключу.
    """

    def __init__(self, redis_adapter: Redis, redis_key: str = 'etl_state'):
        self.redis_adapter = redis_adapter
        self.redis_key = redis_key

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохраняет состояние в Redis."""
        self.redis_adapter.set(self.redis_key, json.dumps(state))

    def retrieve_state(self) -> Dict[str, Any]:
        """Загружает состояние из Redis."""
        data = self.redis_adapter.get(self.redis_key)
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {}
        return {}

class State:
    """
    Класс для работы с состоянием ETL-процесса.
    При инициализации загружает состояние из хранилища и держит его в памяти.
    При изменении — сохраняет обратно в хранилище.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self._state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить значение для ключа и сохранить состояние."""
        self._state[key] = value
        self.storage.save_state(self._state)

    def get_state(self, key: str, default: Any = None) -> Any:
        """Получить значение по ключу из состояния."""
        return self._state.get(key, default)
