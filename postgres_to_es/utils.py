import logging
import random
import time
import psycopg

from contextlib import contextmanager
from functools import wraps
from elasticsearch import Elasticsearch, ConnectionError
from psycopg import OperationalError, ClientCursor
from redis import Redis

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, exceptions=(Exception,), service_name="unknown_service"):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует экспоненциальную задержку с джиттером.
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    jitter = random.uniform(0, t)
                    sleep_time = t + jitter
                    logging.warning(f'Ошибка от сервиса {service_name}: {e}. Повтор через {sleep_time:.2f} сек...')
                    time.sleep(sleep_time)
                    n += 1
                    t = min(start_sleep_time * (factor ** n), border_sleep_time)
        return inner
    return func_wrapper


@backoff(start_sleep_time=1, factor=2, border_sleep_time=8, exceptions=(OperationalError,), service_name="PostgreSQL")
def connect_pg(**kwargs):
    kwargs.setdefault('cursor_factory', ClientCursor)
    return psycopg.connect(**kwargs)


@backoff(start_sleep_time=1, factor=2, border_sleep_time=8, exceptions=(Exception,), service_name="Redis")
def connect_redis(**kwargs):
    r = Redis(**kwargs)
    r.ping()
    return r

@backoff(start_sleep_time=1, factor=2, border_sleep_time=8, exceptions=(ConnectionError,), service_name="Elasticsearch")
def connect_es(**kwargs):
    es = Elasticsearch(**kwargs)
    es.ping()
    return es

@contextmanager
def pg_conn_context(**kwargs):
    conn = connect_pg(**kwargs)
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def redis_conn_context(**kwargs):
    conn = connect_redis(**kwargs)
    try:
        yield conn
    finally:
        conn.close()
