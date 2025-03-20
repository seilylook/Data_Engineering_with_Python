# Kafka Docker 네트워킹 Q&A

## 질문: 같은 네트워크에 있는데 왜 외부 포트를 사용해야 하나요?

**질문 내용: 근데 컨테이너는 각각 python-app, kafka1, kafka2, kafka3 이더라도 아래의 docker-compose보면 network는 같은 data-platform을 공유하도록 설정했는데 왜 외부 포트로 접근해야 되는거야?**

```yaml
services:
  kafka1:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka1
    ports:
      - "9092:9092"
      - "29092:29092"
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG4J_LOGGERS: "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO"
      KAFKA_LOG4J_ROOT_LOGLEVEL: INFO
      KAFKA_TOOLS_LOG4J_LOGLEVEL: INFO
    volumes:
      - kafka1-data:/var/lib/kafka/data
      - ./logs/kafka1:/var/log/kafka
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 45s
    networks:
      - data-platform

  python-app:
    build:
      context: .
      dockerfile: Dockerfile
      cache_from:
        - python-app:latest
    image: python-app:latest
    container_name: python-app
    working_dir: /app
    volumes:
      - ./app/src:/app/src:cached
      - ./app/tests:/app/tests:cached
      - ./app/data:/app/data:cached
      - ./app/config:/app/config:cached
    environment:
      - PYTHONUNBUFFERED=1
      - PYTHONPATH=/app/src
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - data-platform
    deploy:
      resources:
        limits:
          memory: 8G

networks:
  data-platform:
    driver: bridge
```

## 답변: Kafka의 특별한 네트워킹 동작 방식

모든 컨테이너가 같은 'data-platform' 네트워크를 공유하고 있음에도 내부 포트(9092, 9093, 9094)가 아닌 외부 포트(29092, 29093, 29094)로 접근해야 하는 이유는 Kafka의 특별한 네트워킹 동작 방식 때문입니다.

### 핵심 원인: Kafka의 ADVERTISED_LISTENERS 설정

문제의 핵심은 이 설정에 있습니다:
```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092,PLAINTEXT_HOST://localhost:29092
```

여기서 중요한 점은:

1. **advertised.listeners의 의미**:
   - 이 설정은 Kafka가 클라이언트에게 "나에게 연결할 때 이 주소를 사용하세요"라고 알려주는 주소입니다.
   - 클라이언트가 처음 bootstrap 서버에 연결한 후, Kafka는 이 advertised.listeners 값을 반환하고 클라이언트는 **이후 모든 통신에 이 주소를 사용**합니다.

2. **문제 시나리오**:
   - Python 클라이언트가 처음에 `kafka1:9092`로 연결을 시도합니다.
   - 연결에 성공하면 Kafka는 `PLAINTEXT://kafka1:9092`를 반환합니다.
   - 그런데 Docker 컨테이너의 hostname인 `kafka1`은 **다른 컨테이너에서는 해석될 수 있지만, 컨테이너 내부의 애플리케이션 라이브러리에 따라 제대로 해석되지 않을 수 있습니다**.
   - Kafka 클라이언트 라이브러리가 이 호스트명을 해석하지 못하면 연결이 실패합니다.

3. **주소 변환 문제**:
   - Docker 내부 DNS는 컨테이너 간에 서비스 이름을 해석할 수 있게 해주지만, 모든 라이브러리가 이를 제대로 활용하지는 않습니다.
   - Python의 confluent-kafka 라이브러리의 경우, 내부적으로 librdkafka를 사용하는데, 이 C 라이브러리가 Docker의 DNS 해석 메커니즘과 완전히 호환되지 않을 수 있습니다.

### 해결책이 작동하는 이유

외부 포트(`localhost:29092`)로 설정했을 때 작동하는 이유:

1. **이중 리스너의 목적**:
   - `PLAINTEXT://kafka1:9092`: 브로커 간 통신용 (다른 Kafka 브로커가 사용)
   - `PLAINTEXT_HOST://localhost:29092`: 외부 클라이언트 연결용

2. **네트워크 경로**:
   - `localhost:29092`로 연결 시, 요청은 Docker 호스트의 29092 포트로 가고, 이는 다시 Kafka 컨테이너의 9092 포트로 포워딩됩니다.
   - 이 과정에서 호스트명 해석 문제를 우회하게 됩니다.

3. **localhost의 의미**:
   - Docker 컨테이너 내부에서 `localhost`는 컨테이너 자신을 가리킵니다.
   - 그러나 Docker 컨테이너는 포트 포워딩을 통해 호스트 머신과 통신할 수 있습니다.

### 해결을 위한 대안

1. **advertised.listeners 수정**:
   ```yaml
   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka1:9092,PLAINTEXT_HOST://kafka1:9092
   ```
   이렇게 하면 내부 통신용 주소를 반환하도록 할 수 있습니다.

2. **내부 IP 사용**:
   ```yaml
   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://172.18.0.6:9092
   ```
   고정 IP를 사용하면 호스트명 해석 문제를 피할 수 있지만, IP가 변경될 수 있어 권장되지 않습니다.

3. **호스트 머신의 IP 사용** (Docker 네트워크 내 특수 주소):
   ```yaml
   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://host.docker.internal:29092
   ```
   일부 Docker 환경에서는 `host.docker.internal`이 호스트 머신을 가리킵니다.

### 결론

같은 네트워크에 있더라도 Kafka의 advertised.listeners 설정과 클라이언트 라이브러리의 DNS 해석 동작 방식 때문에 내부 포트로 직접 연결이 어려울 수 있습니다. 외부 포트를 사용하는 것이 이러한 복잡성을 우회하는 간단한 해결책입니다.