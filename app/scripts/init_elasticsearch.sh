#!/bin/bash

# Elasticsearch 컨테이너가 실행 중인지 확인
if ! docker ps | grep -q "elasticsearch"; then
    echo "Elasticsearch 컨테이너가 실행되고 있지 않습니다."
    exit 1
fi

# Elasticsearch가 준비될 때까지 대기
echo "Elasticsearch가 준비될 때까지 대기 중..."
until $(curl --silent --output /dev/null --fail --max-time 5 http://localhost:9200); do
    printf '.'
    sleep 5
done
echo "Elasticsearch가 준비되었습니다."

# users 인덱스 설정
echo "users 인덱스 생성 중..."
curl -X PUT "localhost:9200/users" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "name": { "type": "text" },
      "street": { "type": "text" },
      "city": { "type": "text" },
      "zip": { "type": "keyword" },
      "lng": { "type": "double" },
      "lat": { "type": "double" }
    }
  }
}
'

# 결과 확인
if [ $? -eq 0 ]; then
    echo "users 인덱스가 성공적으로 생성되었습니다."
else
    echo "users 인덱스 생성 중 오류가 발생했습니다."
    exit 1
fi

# 인덱스 확인
echo "인덱스 목록 확인:"
curl -X GET "localhost:9200/_cat/indices?v"