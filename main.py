import abc
import json
import os
from typing import Any, Dict
from redis import Redis

class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=4)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        if not os.path.exists(self.file_path):
            return {}
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
        
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
    

# Создаём подключение к Redis (localhost по умолчанию)
redis_adapter = Redis(host="localhost", port=6379, db=0)

# Экземпляр хранилища
storage = RedisStorage(redis_adapter, redis_key="etl_state")  # ключ можешь назвать как хочешь

# Экземпляр State
state = State(storage)

state.set_state("test_key", "2024-08-01T10:00:00.000000")
print(state.get_state("test_key"))  # 123

print(state.get_state("not_exists"))  # None