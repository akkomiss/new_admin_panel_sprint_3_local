#!/bin/sh
# Скрипт для идемпотентного создания индекса в Elasticsearch

# Ждем, пока Elasticsearch не станет доступен
until curl -s --fail http://elasticsearch:9200/_cluster/health?wait_for_status=yellow&timeout=2s > /dev/null; do
  echo "Waiting for Elasticsearch..."
  sleep 2
done

# Проверяем, существует ли индекс
curl -s --head --fail http://elasticsearch:9200/movies >/dev/null
CURL_EXIT_CODE=$?

case $CURL_EXIT_CODE in
  22)
    echo "Index 'movies' not found. Creating..."
    curl -X PUT "http://elasticsearch:9200/movies" -H "Content-Type: application/json" --data-binary "@/app/es_schema.json" --fail-with-body
    ;;
  0)
    echo "Index 'movies' already exists. Skipping."
    ;;
  *)
    echo "Error checking index. Curl exit code: $CURL_EXIT_CODE"
    exit $CURL_EXIT_CODE
    ;;
esac
