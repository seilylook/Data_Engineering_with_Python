from confluent_kafka import Producer, Consumer, KafkaError
import pandas as pd
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Callable, Any, List


class KafkaProducer:
    """CSV 파일의 데이터를 읽어서 Kafka Topic으로 전송하는 클래스"""

    def __init__(
        self,
        bootstrap_servers="localhost:29092,localhost:29093,localhost:29094",
        client_id: str = "csv-producer",
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        """Kafka Producer 초기화

        Args:
            bootstrap_servers: Kafka 브로커 주소들 (쉼표로 구분)
            client_id: Producer 클라이언트 ID
            extra_config: 추가 Kafka Producer 설정
        """
        self.logger = logging.getLogger(__name__)
        self.config = {"bootstrap.servers": bootstrap_servers, "client.id": client_id}

        if extra_config:
            self.config.update(extra_config)

        self.producer = Producer(self.config)

    def delivery_callback(self, err, msg) -> None:
        """메시지 전송 결과 확인 콜백 함수"""
        if err is not None:
            self.logger.error(f"메시지 전송 실패: {err}")
        else:
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            self.logger.debug(f"메시지 전송 성공: {topic} [{partition}] @ {offset}")

    def check_topic_exists(self, topic_name: str) -> bool:
        """
        지정된 토픽이 Kafka에 존재하는지 확인

        Args:
            topic_name: 확인할 토픽 이름

        Returns:
            bool: 토픽 존재 여부
        """
        topics = self.producer.list_topics().topics
        exists = topic_name in topics

        if not exists:
            self.logger.warning(
                f"'{topic_name}' 토픽이 존재하지 않습니다. 자동 생성될 수 있습니다."
            )

        return exists

    def process_csv(
        self,
        csv_path: str,
        topic_name: str,
        batch_size: int = 100,
        key_field: Optional[str] = None,
        transform_func: Optional[Callable[[Dict], Dict]] = None,
    ) -> int:
        """
        CSV 파일을 읽어 Kafka 토픽으로 전송

        Args:
            csv_path: CSV 파일 경로
            topic_name: 전송할 Kafka 토픽 이름
            batch_size: 한 번에 처리할 CSV 레코드 수
            key_field: 메시지 키로 사용할 CSV 필드명 (지정하지 않으면 키 없음)
            transform_func: 각 레코드에 적용할 변환 함수 (선택 사항)

        Returns:
            int: 전송된 메시지 수
        """
        # CSV 파일 존재 여부 확인
        csv_file = Path(csv_path)
        if not csv_file.exists():
            self.logger.error(f"CSV 파일을 찾을 수 없습니다: {csv_path}")
            return 0

        try:
            self.logger.info("Kafka 클러스터 연결 및 토픽 확인 중...")
            self.check_topic_exists(topic_name)

            self.logger.info(f"'{csv_path}' 파일 처리 시작")
            messages_sent = 0

            for chunk in pd.read_csv(csv_path, chunksize=batch_size):
                for _, row in chunk.iterrows():
                    # 레코드를 딕셔너리로 변환
                    record_dict = row.to_dict()

                    # 변환 함수가 지정되었으면 적용
                    if transform_func:
                        record_dict = transform_func(record_dict)

                    # 메시지 값을 JSON으로 직렬화
                    message_value = json.dumps(record_dict).encode("utf-8")

                    # 키 필드가 지정되었으면 키 설정
                    message_key = None
                    if key_field and key_field in record_dict:
                        message_key = str(record_dict[key_field]).encode("utf-8")

                    # 메시지 전송 (비동기)
                    self.producer.produce(
                        topic=topic_name,
                        value=message_value,
                        key=message_key,
                        callback=self.delivery_callback,
                    )
                    messages_sent += 1

                    # 주기적으로 폴링하여 콜백 처리
                    self.producer.poll(0)

                self.logger.info(f"{messages_sent}개 메시지 처리 중...")

            # 남은 메시지 전송 보장
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                self.logger.warning(
                    f"{remaining}개의 메시지가 지정된 시간 내에 전송되지 않았습니다."
                )

            self.logger.info(
                f"총 {messages_sent}개 메시지가 '{topic_name}' 토픽으로 전송되었습니다."
            )
            return messages_sent

        except Exception as e:
            self.logger.error(f"메시지 전송 중 오류 발생: {e}")
            return 0

    def close(self):
        """Producer 리소스 정리"""
        self.producer.flush()
        # confluent_kafka.Producer는 명시적 close 메서드가 없지만
        # GC가 처리하도록 참조 해제


class KafkaConsumer:
    """Kafka Topic의 메시지를 소비하는 클래스"""

    def __init__(
        self,
        bootstrap_servers="localhost:29092,localhost:29093,localhost:29094",
        group_id="data-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        """Kafka Consumer 초기화

        Args:
            bootstrap_servers: Kafka 브로커 주소들 (쉼표로 구분)
            group_id: Consumer 그룹 ID
            auto_offset_reset: 오프셋 리셋 정책 ('earliest', 'latest')
            enable_auto_commit: 자동 커밋 활성화 여부
            extra_config: 추가 Kafka Consumer 설정
        """
        self.logger = logging.getLogger(__name__)
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
        }

        if extra_config:
            self.config.update(extra_config)

        self.consumer = None

    def connect(self, topics: List[str]):
        """Consumer 연결 및 토픽 구독 시작

        Args:
            topics: 구독할 토픽 목록
        """
        try:
            self.consumer = Consumer(self.config)
            self.consumer.subscribe(topics)
            self.logger.info(f"토픽 구독 시작: {', '.join(topics)}")
        except Exception as e:
            self.logger.error(f"Consumer 초기화 중 오류 발생: {e}")
            raise

    def consume_messages(
        self,
        process_func: Callable[[Dict], None],
        timeout: float = 1.0,
        max_messages: int = -1,
        stop_condition: Optional[Callable[[], bool]] = None,
    ) -> int:
        """메시지 소비 처리

        Args:
            process_func: 메시지 처리 함수 (딕셔너리를 인자로 받음)
            timeout: 메시지 폴링 타임아웃 (초)
            max_messages: 최대 처리할 메시지 수 (-1은 무제한)
            stop_condition: 소비 중단 조건 함수

        Returns:
            int: 처리된 메시지 수
        """
        if not self.consumer:
            self.logger.error(
                "Consumer가 초기화되지 않았습니다. connect() 메서드를 먼저 호출하세요."
            )
            return 0

        messages_processed = 0
        running = True

        try:
            self.logger.info("메시지 소비 시작...")

            while running:
                # 메시지 폴링
                msg = self.consumer.poll(timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 파티션의 끝에 도달함 (일반적인 상황)
                        self.logger.debug(
                            f"파티션 {msg.partition()} 끝에 도달: {msg.topic()}"
                        )
                    else:
                        # 오류 발생
                        self.logger.error(f"메시지 소비 중 오류: {msg.error()}")
                    continue

                # 메시지 값 디코딩 및 처리
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    # 사용자 정의 처리 함수 호출
                    process_func(value)
                    messages_processed += 1

                    # 처리 상태 로깅 (100개 단위)
                    if messages_processed % 100 == 0:
                        self.logger.info(f"{messages_processed}개 메시지 처리됨")

                except Exception as e:
                    self.logger.error(
                        f"메시지 처리 중 오류: {e}, 메시지: {msg.value()}"
                    )

                # 중단 조건 확인
                if max_messages > 0 and messages_processed >= max_messages:
                    self.logger.info(
                        f"최대 메시지 수 {max_messages}개에 도달하여 소비 중단"
                    )
                    break

                if stop_condition and stop_condition():
                    self.logger.info("중단 조건에 도달하여 소비 중단")
                    break

            self.logger.info(f"총 {messages_processed}개 메시지 처리 완료")
            return messages_processed

        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 소비 중단")
            return messages_processed
        except Exception as e:
            self.logger.error(f"메시지 소비 중 예외 발생: {e}")
            return messages_processed

    def consume_to_dataframe(
        self, max_messages: int = 1000, timeout: float = 10.0
    ) -> pd.DataFrame:
        """메시지를 소비하여 Pandas DataFrame으로 변환

        Args:
            max_messages: 수집할 최대 메시지 수
            timeout: 각 메시지 폴링 타임아웃 (초)

        Returns:
            pd.DataFrame: 수집된 메시지로 구성된 DataFrame
        """
        records = []

        def collect_records(message_dict):
            records.append(message_dict)

        self.consume_messages(
            process_func=collect_records, timeout=timeout, max_messages=max_messages
        )

        if not records:
            self.logger.warning("수집된 레코드가 없습니다.")
            return pd.DataFrame()

        return pd.DataFrame(records)

    def commit(self):
        """명시적으로 현재 오프셋 커밋"""
        if self.consumer:
            self.consumer.commit()
            self.logger.debug("현재 오프셋 커밋 완료")

    def seek_to_beginning(self, partitions=None):
        """특정 파티션의 처음으로 오프셋 이동

        Args:
            partitions: 이동할 파티션 목록 (None이면 모든 파티션)
        """
        if not self.consumer:
            self.logger.error("Consumer가 초기화되지 않았습니다.")
            return

        if partitions is None:
            # 현재 할당된 모든 파티션 가져오기
            partitions = self.consumer.assignment()

        self.consumer.seek_to_beginning(partitions)
        self.logger.info(f"{len(partitions)}개 파티션의 처음으로 오프셋 이동")

    def close(self):
        """Consumer 리소스 정리"""
        if self.consumer:
            self.consumer.close()
            self.logger.debug("Consumer 연결 종료")
            self.consumer = None
