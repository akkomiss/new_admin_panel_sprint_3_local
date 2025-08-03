import logging
import os
import time
import json
from psycopg import OperationalError
from dotenv import load_dotenv

from utils import pg_conn_context, redis_conn_context
from state import State, RedisStorage
from producer import PostgresProducer
from enricher import PostgresEnricher
from merger import PostgresMerger
from transformer import Transformer

def process_source(config, producers, enricher, merger, transformer, r_conn):
    """
    Выполняет полный цикл ETL для одного источника данных (одной таблицы).
    """
    source_type = config['source_type']
    producer = producers[source_type]
    
    # 1. Extract
    logging.info(f"-> Запуск producer для '{source_type}'...")
    source_rows = producer.extract()
    if not source_rows:
        logging.info(f"Для '{source_type}' нет новых данных.")
        return

    logging.info(f"Producer извлек {len(source_rows)} записей из '{source_type}'.")
    
    # 2. Enrich
    source_ids = [row[0] for row in source_rows]
    film_work_ids = enricher.enrich(source_ids, source_type) if config['enrich'] else source_ids
    
    # 3. Merge -> Transform -> Load
    if film_work_ids:
        logging.info(f"Подготовка к обработке данных для {len(film_work_ids)} фильмов.")
        
        raw_data = merger.fetch_merged_data(film_work_ids)
        transformed_data = transformer.transform_data(raw_data)
        
        if transformed_data:
            output_queue_name = 'processed_movies_queue'
            logging.info(f"Отправка {len(transformed_data)} документов в очередь '{output_queue_name}'...")
            for doc in transformed_data:
                r_conn.rpush(output_queue_name, json.dumps(doc, default=str))
        else:
            logging.warning("Данные не были трансформированы, т.к. transformer вернул пустой результат.")
    else:
        logging.warning(f"После обогащения для '{source_type}' не осталось фильмов для обработки.")

    # 4. Update State
    last_modified, last_id = source_rows[-1][1], str(source_rows[-1][0])
    producer.state.set_state('last_updated_at', str(last_modified))
    producer.state.set_state('last_id', last_id)
    logging.info(f"Состояние для '{source_type}' обновлено: modified={last_modified}, id={last_id}\n")

def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('SQL_HOST'),
        'port': os.getenv('SQL_PORT')
    }
    
    redis_opts = {'host': 'localhost', 'port': 6379, 'db': 0, 'decode_responses': True}
    
    producer_configs = [
        {'source_type': 'film_work', 'table': 'content.film_work', 'state_key': 'film_work_producer', 'enrich': False},
        {'source_type': 'person', 'table': 'content.person', 'state_key': 'person_producer', 'enrich': True},
        {'source_type': 'genre', 'table': 'content.genre', 'state_key': 'genre_producer', 'enrich': True},
    ]

    try:
        with redis_conn_context(**redis_opts) as r_conn:
            logging.info("Соединение с Redis установлено и будет поддерживаться.")
            
            transformer = Transformer()

            while True:
                try:
                    with pg_conn_context(**dsl) as p_conn:
                        # Инициализируем компоненты с новым соединением
                        states = {config['state_key']: State(RedisStorage(r_conn, config['state_key'])) for config in producer_configs}
                        producers = {config['source_type']: PostgresProducer(p_conn, states[config['state_key']], config['table']) for config in producer_configs}
                        enricher = PostgresEnricher(p_conn)
                        merger = PostgresMerger(p_conn)
                        
                        # Главный цикл теперь просто вызывает обработчик для каждого источника
                        for config in producer_configs:
                            process_source(config, producers, enricher, merger, transformer, r_conn)

                except OperationalError as e:
                    logging.warning(f"Не удалось подключиться к PostgreSQL в этом цикле. Повтор через 5 секунд. Ошибка: {e}")
                
                logging.info("--- Все источники обработаны. Пауза 5 секунд. ---\n")
                time.sleep(1)

    except Exception as e:
        logging.error(f"Критическая ошибка в главном цикле ETL: {e}", exc_info=True)


if __name__ == '__main__':
    main()
