import psycopg
import logging
import os
import random
import time

from psycopg import OperationalError, ClientCursor
from dotenv import load_dotenv
from redis import Redis
from contextlib import contextmanager
from functools import wraps

from state import State, RedisStorage
from enricher import PostgresEnricher

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, exceptions=(Exception,), service_name="unknown_service"):
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
                    print(f'Ошибка от сервиса {service_name}: {e}. Повтор через {sleep_time:.2f} сек...')
                    time.sleep(sleep_time)
                    n += 1
                    t = min(start_sleep_time * (factor ** n), border_sleep_time)
        return inner
    return func_wrapper

@backoff(start_sleep_time=1, factor=2, border_sleep_time=8, exceptions=(OperationalError,), service_name="PostgreSQL")
def connect_pg(**kwargs):
    kwargs.setdefault('cursor_factory', ClientCursor)
    return psycopg.connect(**kwargs)

@backoff(start_sleep_time=1, factor=2, border_sleep_time=8, exceptions=(OperationalError,), service_name="Redis")
def connect_redis(**kwargs):
    return Redis(**kwargs)

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

def main():
    # Загружаем переменные окружения из .env
    load_dotenv()

    # Параметры подключения к PostgreSQL
    dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('SQL_HOST'),
        'port': os.getenv('SQL_PORT')
    }    

     # Список конфигураций для каждой таблицы:
    configs = [
        {
            'queue_name': 'person_ids',
            'state_key': 'person_enricher_state',
            'relation_field': 'person_id',
            'target_queue': 'film_work_ids'
        },
        {
            'queue_name': 'genre_ids',
            'state_key': 'genre_enricher_state',
            'relation_field': 'genre_id',
            'target_queue': 'film_work_ids'
        }
    ]

    with redis_conn_context(host='localhost', port=6379, db=0) as r, pg_conn_context(**dsl) as pg_conn:
        for config in configs:
            storage = RedisStorage(r, redis_key=config['state_key'])
            state = State(storage)
            enricher = PostgresEnricher(pg_conn, state, config['queue_name'], config['target_queue'])
            enrich_flag = f"enrich_ready:{config['queue_name']}"

            while True:
                print(f"Enricher ждёт флаг для {config['queue_name']}...")
                while not r.get(enrich_flag):
                    time.sleep(0.5)

                processed = enricher.fetch_and_enrich(r)
                if not processed:
                    print(f"Enricher: очередь {config['queue_name']} пуста, завершаем обработку.")
                    r.delete(enrich_flag)
                    break

                r.delete(enrich_flag)
                print(f"Enricher: пачка обработана и флаг {enrich_flag} снят\n")

if __name__ == '__main__':
    main()
