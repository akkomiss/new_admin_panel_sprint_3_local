import psycopg
import logging
import os
import random
import time

from psycopg import OperationalError, ClientCursor
from dotenv import load_dotenv
from redis import Redis
from contextlib import contextmanager
from typing import Generator, List, Optional, Any, Type
from dataclasses import astuple, fields
from functools import wraps

from state import State, RedisStorage
from models import film_work, genre, person, genre_film_work, person_film_work

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, exceptions=(Exception,)):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    # Jitter: добавляем случайное слагаемое от 0 до t
                    jitter = random.uniform(0, t)
                    sleep_time = t + jitter
                    print(f'Ошибка: {e}. Повтор через {sleep_time:.2f} сек...')
                    time.sleep(sleep_time)
                    n += 1
                    t = min(start_sleep_time * (factor ** n), border_sleep_time)
        return inner
    return func_wrapper

@backoff(start_sleep_time=1, factor=2, border_sleep_time=8, exceptions=(OperationalError,))
def connect_pg(**kwargs):
    kwargs.setdefault('cursor_factory', ClientCursor)
    return psycopg.connect(**kwargs)

@contextmanager
def pg_conn_context(**kwargs):
    conn = connect_pg(**kwargs)
    try:
        yield conn
    finally:
        conn.close()

def extract_data():
    pass

def main():
    # Загружаем переменные окружения из .env
    load_dotenv()

    # Параметры подключения к PostgreSQL
    dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT')
    }    

    with pg_conn_context(**dsl) as pg_conn:
        pass

    # Создаём подключение к Redis (localhost по умолчанию)
    redis_adapter = Redis(host="localhost", port=6379, db=0)

    # Экземпляр хранилища
    storage = RedisStorage(redis_adapter, redis_key="etl_state")  # ключ можешь назвать как хочешь

    # Экземпляр State
    state = State(storage)

    state.set_state("test_key", "2024-08-01T10:00:00.000000")
    print(state.get_state("test_key"))  # 123

    print(state.get_state("not_exists"))  # None


if __name__ == '__main__':
    main()
