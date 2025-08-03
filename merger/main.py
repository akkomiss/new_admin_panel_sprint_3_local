import psycopg
import logging
import os
import random
import time
import json

from psycopg import OperationalError, ClientCursor
from dotenv import load_dotenv
from redis import Redis
from contextlib import contextmanager
from functools import wraps
from state import State, RedisStorage
from merger import PostgresMerger

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

    configs = [
        {
            'queue_key': 'film_work_ids',
            'state_key': 'film_work_merger_state',
            'batch_size': 100
        }
    ]

    with redis_conn_context(host='localhost', port=6379, db=0) as r, pg_conn_context(**dsl) as pg_conn:
        for config in configs:
            storage = RedisStorage(r, redis_key=config['state_key'])
            state = State(storage)
            merger = PostgresMerger(
                pg_conn=pg_conn,
                state=state,
                queue_key=config['queue_key'],
                batch_size=config.get('batch_size', 100)
            )
            enrich_flag = f"enrich_ready:{config['queue_key']}"

            print(f"Merger: стартует для очереди {config['queue_key']}")
            while True:
                print(f"Merger ждёт флаг для {config['queue_key']}...")
                while not r.get(enrich_flag):
                    time.sleep(0.5)

                last_merged_ids = state.get_state('last_merged_ids')
                print(f"Merger: последнее обработанное состояние: {last_merged_ids}")

                rows = merger.fetch_and_merge(r)
                if not rows:
                    print(f"Merger: очередь {config['queue_key']} пуста, ждём новые данные...")
                    r.delete(enrich_flag)
                    time.sleep(2)  # Подожди пару секунд и снова попробуй
                    continue
                
                # Здесь этап трансформации и загрузки в ES, если надо
                redis_list_key = "film_work_rows_queue"
                for row in rows:
                    # Сериализуем row в строку JSON для хранения
                    r.rpush(redis_list_key, json.dumps(row, default=str))

                r.delete(enrich_flag)
                print(f"Merger: обработано {len(rows)} строк\n")


if __name__ == '__main__':
    main()
