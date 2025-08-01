from state import State, RedisStorage
from redis import Redis
from models import film_work, genre, person, genre_film_work, person_film_work


# Создаём подключение к Redis (localhost по умолчанию)
redis_adapter = Redis(host="localhost", port=6379, db=0)

# Экземпляр хранилища
storage = RedisStorage(redis_adapter, redis_key="etl_state")  # ключ можешь назвать как хочешь

# Экземпляр State
state = State(storage)

state.set_state("test_key", "2024-08-01T10:00:00.000000")
print(state.get_state("test_key"))  # 123

print(state.get_state("not_exists"))  # None