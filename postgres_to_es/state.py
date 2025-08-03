import json
from utils import backoff

class State:
    def __init__(self, storage):
        self.storage = storage

    def set_state(self, key, value):
        self.storage.save_state({key: value})

    def get_state(self, key, default=None):
        return self.storage.retrieve_state().get(key, default)

class RedisStorage:
    def __init__(self, redis_adapter, redis_key='etl_state'):
        self.redis_adapter = redis_adapter
        self.redis_key = redis_key

    @backoff(exceptions=(Exception,), service_name="Redis")
    def save_state(self, state):
        current_state = self.retrieve_state()
        current_state.update(state)
        self.redis_adapter.set(self.redis_key, json.dumps(current_state))

    @backoff(exceptions=(Exception,), service_name="Redis")
    def retrieve_state(self):
        data = self.redis_adapter.get(self.redis_key)
        if data:
            return json.loads(data)
        return {}
