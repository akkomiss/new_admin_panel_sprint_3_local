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
from extractor import PostgresExtractor

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
            'table': 'film_work',
            'state_key': 'film_work_producer',
            'queue_name': 'film_work_ids'
        },
        {
            'table': 'person',
            'state_key': 'person_producer',
            'queue_name': 'person_ids'
        },
        {
            'table': 'genre',
            'state_key': 'genre_producer',
            'queue_name': 'genre_ids'
        }
    ]

    with redis_conn_context(host='localhost', port=6379, db=0) as r, pg_conn_context(**dsl) as pg_conn:
        for config in configs:
            storage = RedisStorage(r, redis_key=config['state_key'])
            state = State(storage)

            #Проверяем, завершён ли initial для этой таблицы
            is_initial_completed = state.get_state('initial_completed')
            table = config['table']

            # Если initial ещё НЕ завершён и НЕ таблица film_work — пропустить, иначе сразу работать.
            if not is_initial_completed and table != 'film_work':
                print(f"Пропускаем таблицу {table} — ждем завершения initial для film_work.")
                continue

            extractor = PostgresExtractor(pg_conn, state, table=config['table'], batch_size=100)
            print(f"Обрабатываем таблицу: {config['table']}")
            queue_key = config['queue_name']
            enrich_flag = f"enrich_ready:{queue_key}"

            while True:
                processed = extractor.extract(r, queue_key)
                if not processed:
                    print(f"Всё обработано для {config['table']}\n")
                    # Только для film_work выставляем initial_completed!
                    if table == 'film_work' and not is_initial_completed:
                        state.set_state('initial_completed', True)
                    break

                # Ставим флаг "пачка готова"
                r.set(enrich_flag, 1)

                # Ждём, пока Enricher снимет флаг
                print(f"Producer: ждёт, когда Enricher обработает очередь {queue_key}...")
                while r.get(enrich_flag):
                    time.sleep(0.5)
                print(f"Producer: Enricher обработал пачку из {queue_key}, продолжаем.")

if __name__ == '__main__':
    main()
