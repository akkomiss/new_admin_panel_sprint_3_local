import abc
import json
import os
from typing import Any, Dict
from redis import Redis

class BaseStorage(abc.ABC):

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""

        
class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis, redis_key: str = "state") -> None:
        self.redis_adapter = redis_adapter
        self.redis_key = redis_key

    def save_state(self, state: Dict[str, Any]) -> None:
        # Сохраняем словарь как строку JSON под одним ключом
        json_state = json.dumps(state)
        self.redis_adapter.set(self.redis_key, json_state)

    def retrieve_state(self) -> Dict[str, Any]:
        data = self.redis_adapter.get(self.redis_key)
        if data is None:
            return {}
        try:
            # В redis-py .get возвращает bytes, декодируем в строку
            return json.loads(data.decode('utf-8'))
        except Exception:
            return {}

class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self._state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self._state[key] = value
        self.storage.save_state(self._state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self._state.get(key)