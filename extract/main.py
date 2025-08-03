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
    
    # Конфигурации для каждого типа источника
    producer_configs = [
        {'source_type': 'film_work', 'table': 'content.film_work', 'state_key': 'film_work_producer', 'enrich': False},
        {'source_type': 'person', 'table': 'content.person', 'state_key': 'person_producer', 'enrich': True},
        {'source_type': 'genre', 'table': 'content.genre', 'state_key': 'genre_producer', 'enrich': True},
    ]

    try:
        with redis_conn_context(**redis_opts) as r_conn:
            logging.info("Соединение с Redis установлено и будет поддерживаться.")
            
            while True:
                try:
                    with pg_conn_context(**dsl) as p_conn:
                        states = {config['state_key']: State(RedisStorage(r_conn, config['state_key'])) for config in producer_configs}
                        producers = {config['source_type']: PostgresProducer(p_conn, states[config['state_key']], config['table']) for config in producer_configs}
                        enricher = PostgresEnricher(p_conn)
                        merger = PostgresMerger(p_conn)
                        
                        for config in producer_configs:
                            source_type = config['source_type']
                            producer = producers[source_type]
                            
                            logging.info(f"-> Запуск producer для '{source_type}'...")
                            source_rows = producer.extract()
                            
                            if not source_rows:
                                logging.info(f"Для '{source_type}' нет новых данных.")
                                continue
                            
                            logging.info(f"Producer извлек {len(source_rows)} записей из '{source_type}'.")
                            source_ids = [row[0] for row in source_rows]
                            
                            film_work_ids_to_merge = []
                            
                            if config['enrich']:
                                logging.info(f"-> Запуск enricher для '{source_type}'...")
                                film_work_ids_to_merge = enricher.enrich(source_ids, source_type)
                                logging.info(f"Enricher нашел {len(film_work_ids_to_merge)} связанных фильмов.")
                            else:
                                film_work_ids_to_merge = source_ids
                                
                            if not film_work_ids_to_merge:
                                logging.warning(f"После обогащения для '{source_type}' не осталось фильмов для обработки.")
                            else:
                                logging.info(f"-> Запуск merger для {len(film_work_ids_to_merge)} фильмов...")
                                merged_data = merger.merge(film_work_ids_to_merge)
                                
                                if merged_data:
                                    output_queue_name = 'processed_movies_queue'
                                    logging.info(f"Merger собрал {len(merged_data)} полных документов. Отправка в очередь '{output_queue_name}'...")
                                    for doc in merged_data:
                                        r_conn.rpush(output_queue_name, json.dumps(doc, default=str))
                                else:
                                    logging.warning("Merger не вернул данных после обработки.")
                            
                            last_modified, last_id = source_rows[-1][1], str(source_rows[-1][0])
                            producer.state.set_state('last_updated_at', str(last_modified))
                            producer.state.set_state('last_id', last_id)
                            logging.info(f"Состояние для '{source_type}' обновлено: modified={last_modified}, id={last_id}\n")

                except OperationalError as e:
                    logging.warning(f"Не удалось подключиться к PostgreSQL в этом цикле. Повтор через 5 секунд. Ошибка: {e}")
                
                logging.info("--- Все источники обработаны. Пауза 5 секунд. ---\n")
                time.sleep(1)

    except Exception as e:
        logging.error(f"Критическая ошибка в главном цикле ETL: {e}", exc_info=True)


if __name__ == '__main__':
    main()
