from contextlib import contextmanager
from typing import Generator, Optional
import logging
import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch
from src.config.database import PostgresConfig, ElasticsearchConfig

logger = logging.getLogger(__name__)


class PostgresConnector:
    """PostgreSQL 데이터베이스 연결 관리자"""

    def __init__(self, config: Optional[PostgresConfig] = None):
        """데이터베이스 연결 관리자 초기화

        Args:
            config: PostgreSQL 설정. None일 경우 기본값 사용
        """
        self.config = config or PostgresConfig()
        self._conn: Optional[connection] = None
        self._cur: Optional[cursor] = None

    @contextmanager
    def connect(self) -> Generator[cursor, None, None]:
        """데이터베이스 연결 컨텍스트 매니저

        Yields:
            psycopg2 커서 객체

        Raises:
            psycopg2.Error: 데이터베이스 연결 또는 쿼리 실행 중 오류 발생시
        """
        try:
            self._conn = psycopg2.connect(self.config.connection_string)
            self._cur = self._conn.cursor(cursor_factory=DictCursor)
            yield self._cur
            self._conn.commit()

        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if self._conn:
                self._conn.rollback()
            raise

        finally:
            if self._cur:
                self._cur.close()
            if self._conn:
                self._conn.close()

    def check_connection(self) -> bool:
        """데이터베이스 연결 확인

        Returns:
            연결 성공 여부
        """
        try:
            with self.connect() as cur:
                cur.execute("SELECT 1")
                logger.info("PostgreSQL 연결 성공!")
                return True
        except Exception as e:
            logger.error(f"PostgreSQL 연결 실패: {e}")
            return False


class ElasticsearchConnector:
    """Elasticsearch 연결 관리자"""

    def __init__(self, config: Optional[ElasticsearchConfig] = None):
        """Elasticsearch 연결 관리자 초기화

        Args:
            config: Elasticsearch 설정. None일 경우 기본값 사용
        """
        self.config = config or ElasticsearchConfig()
        self.client = None
        # 테스트 가능하도록 클래스 생성자에서 _connect 메서드를 호출하지 않고
        # 명시적으로 connect() 메서드를 제공합니다.
        self.connect()

    def connect(self) -> None:
        """Elasticsearch에 연결"""
        try:
            self.client = self._create_client()
            logger.info("Elasticsearch 클라이언트 생성 완료")
        except Exception as e:
            logger.error(f"Elasticsearch 클라이언트 생성 실패: {e}")
            self.client = None

    def _create_client(self) -> Elasticsearch:
        """Elasticsearch 클라이언트 객체 생성

        이 메서드를 분리하여 테스트에서 모의 객체로 대체할 수 있도록 함

        Returns:
            Elasticsearch 클라이언트
        """
        return Elasticsearch(self.config.hosts)

    def get_client(self) -> Optional[Elasticsearch]:
        """Elasticsearch 클라이언트 반환

        Returns:
            Elasticsearch 클라이언트 또는 None
        """
        return self.client

    def check_connection(self) -> bool:
        """Elasticsearch 연결 확인

        Returns:
            연결 성공 여부
        """
        if not self.client:
            return False

        try:
            info = self.client.info()
            logger.info(
                f"Elasticsearch 연결 성공! 버전: {info.get('version', {}).get('number', 'unknown')}"
            )
            return True
        except Exception as e:
            logger.error(f"Elasticsearch 연결 실패: {e}")
            return False
