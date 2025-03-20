import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import execute_batch
from elasticsearch import Elasticsearch
from src.utils.connection import PostgresConnector, ElasticsearchConnector

logger = logging.getLogger(__name__)


class Repository(ABC):
    """데이터 저장소 추상 기본 클래스"""

    @abstractmethod
    def save(self, data: Any) -> Any:
        """데이터 저장"""
        pass

    @abstractmethod
    def get(self, id: Any) -> Any:
        """데이터 조회"""
        pass

    @abstractmethod
    def get_all(self, limit: int = 100) -> List[Any]:
        """데이터 목록 조회"""
        pass

    @abstractmethod
    def bulk_save(self, data_list: List[Any]) -> Any:
        """데이터 대량 저장"""
        pass

    @abstractmethod
    def check_connection(self) -> bool:
        """저장소 연결 확인"""
        pass


class PostgreSQLRepository(Repository):
    """PostgreSQL 데이터 저장소"""

    def __init__(self, connector: Optional[PostgresConnector] = None):
        """초기화

        Args:
            connector: 데이터베이스 연결 객체. None일 경우 기본값 사용
        """
        self.connector = connector or PostgresConnector()
        self.logger = logging.getLogger(__name__)

    def save(self, data: Dict[str, Any]) -> int:
        """사용자 데이터 저장

        Args:
            data: 저장할 사용자 데이터 딕셔너리
                {"name": "name1", "street": "street1", "city": "city1", "zip": "zip1", "lng": "lng1", "lat": "lat1"}

        Returns:
            저장된 데이터의 ID
        """
        query = """
            INSERT INTO users (name, street, city, zip, lng, lat) 
            VALUES (%(name)s, %(street)s, %(city)s, %(zip)s, %(lng)s, %(lat)s)
            RETURNING id
        """

        with self.connector.connect() as cur:
            cur.execute(query, data)
            result = cur.fetchone()
            user_id = result["id"]
            self.logger.info(f"Inserted user with ID: {user_id}")
            return user_id

    def get(self, id: int) -> Optional[Dict[str, Any]]:
        """사용자 데이터 조회

        Args:
            id: 조회할 사용자 ID

        Returns:
            사용자 정보 딕셔너리 또는 None
        """
        query = "SELECT * FROM users WHERE id = %s"

        with self.connector.connect() as cur:
            cur.execute(query, (id,))
            result = cur.fetchone()
            return dict(result) if result else None

    def get_all(self, limit: int = 100) -> List[Dict[str, Any]]:
        """모든 사용자 데이터 조회

        Args:
            limit: 조회할 최대 사용자 수

        Returns:
            사용자 정보 딕셔너리 리스트
        """
        query = "SELECT * FROM users LIMIT %s"

        with self.connector.connect() as cur:
            cur.execute(query, (limit,))
            results = cur.fetchall()
            return [dict(row) for row in results]

    def bulk_save(self, data_list: List[Dict[str, Any]]) -> int:
        """사용자 데이터 대량 저장

        Args:
            data_list: 저장할 사용자 데이터 딕셔너리 목록

        Returns:
            저장된 데이터 개수
        """
        query = """
            INSERT INTO users (name, street, city, zip, lng, lat) 
            VALUES (%(name)s, %(street)s, %(city)s, %(zip)s, %(lng)s, %(lat)s)
            RETURNING id
        """

        with self.connector.connect() as cur:
            execute_batch(cur, query, data_list, page_size=1000)
            self.logger.info(f"Bulk inserted {len(data_list)} users")
            return len(data_list)

    def check_connection(self) -> bool:
        """데이터베이스 연결 상태 확인

        Returns:
            연결 성공 여부
        """
        return self.connector.check_connection()


class ElasticsearchRepository(Repository):
    """Elasticsearch 데이터 저장소"""

    def __init__(
        self, connector: Optional[ElasticsearchConnector] = None, index: str = "users"
    ):
        """초기화

        Args:
            connector: Elasticsearch 연결 객체. None일 경우 기본값 사용
            index: 사용할 인덱스 이름
        """
        self.connector = connector or ElasticsearchConnector()
        self.index = index
        self.logger = logging.getLogger(__name__)

    def save(self, data: Dict[str, Any]) -> str:
        """사용자 데이터 저장

        Args:
            data: 저장할 사용자 데이터 딕셔너리

        Returns:
            저장된 데이터의 ID
        """
        client = self.connector.get_client()
        if not client:
            self.logger.error("Elasticsearch 클라이언트가 초기화되지 않았습니다.")
            return None

        try:
            response = client.index(index=self.index, document=data)
            doc_id = response["_id"]
            self.logger.info(f"Elasticsearch에 사용자 저장 완료, ID: {doc_id}")
            return doc_id
        except Exception as e:
            self.logger.error(f"Elasticsearch 저장 실패: {e}")
            return None

    def get(self, id: str) -> Optional[Dict[str, Any]]:
        """사용자 데이터 조회

        Args:
            id: 조회할 문서 ID

        Returns:
            사용자 정보 딕셔너리 또는 None
        """
        client = self.connector.get_client()
        if not client:
            self.logger.error("Elasticsearch 클라이언트가 초기화되지 않았습니다.")
            return None

        try:
            response = client.get(index=self.index, id=id)
            return response["_source"]
        except Exception as e:
            self.logger.error(f"Elasticsearch 조회 실패: {e}")
            return None

    def get_all(self, limit: int = 100) -> List[Dict[str, Any]]:
        """모든 사용자 데이터 조회

        Args:
            limit: 조회할 최대 사용자 수

        Returns:
            사용자 정보 딕셔너리 리스트
        """
        client = self.connector.get_client()
        if not client:
            self.logger.error("Elasticsearch 클라이언트가 초기화되지 않았습니다.")
            return []

        try:
            query = {"query": {"match_all": {}}, "size": limit}
            response = client.search(index=self.index, body=query)
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as e:
            self.logger.error(f"Elasticsearch 전체 조회 실패: {e}")
            return []

    def bulk_save(self, data_list: List[Dict[str, Any]]) -> int:
        """사용자 데이터 대량 저장

        Args:
            data_list: 저장할 사용자 데이터 딕셔너리 목록

        Returns:
            저장된 데이터 개수
        """
        client = self.connector.get_client()
        if not client:
            self.logger.error("Elasticsearch 클라이언트가 초기화되지 않았습니다.")
            return 0

        try:
            body = []
            for data in data_list:
                # 인덱스 작업 정의
                body.append({"index": {"_index": self.index}})
                # 실제 문서 데이터
                body.append(data)

            if body:
                response = client.bulk(operations=body, refresh=True)
                if response.get("errors", True):
                    failed = sum(
                        1 for item in response["items"] if item["index"].get("error")
                    )
                    self.logger.warning(
                        f"Elasticsearch 벌크 저장 중 {failed}개 항목 실패"
                    )
                    return len(data_list) - failed
                else:
                    self.logger.info(
                        f"Elasticsearch에 {len(data_list)}개 문서 벌크 저장 완료"
                    )
                    return len(data_list)
            return 0
        except Exception as e:
            self.logger.error(f"Elasticsearch 벌크 저장 실패: {e}")
            return 0

    def check_connection(self) -> bool:
        """Elasticsearch 연결 상태 확인

        Returns:
            연결 성공 여부
        """
        return self.connector.check_connection()


class RepositoryFactory:
    """Repository 생성 팩토리"""

    @staticmethod
    def create(repo_type: str, **kwargs) -> Repository:
        """Repository 생성

        Args:
            repo_type: 저장소 유형 ("postgresql" 또는 "elasticsearch")
            **kwargs: 저장소 생성에 필요한 추가 인자

        Returns:
            Repository 인스턴스

        Raises:
            ValueError: 지원하지 않는 저장소 유형일 경우
        """
        if repo_type.lower() == "postgresql":
            connector = kwargs.get("connector")
            return PostgreSQLRepository(connector)
        elif repo_type.lower() == "elasticsearch":
            connector = kwargs.get("connector")
            index = kwargs.get("index", "users")
            return ElasticsearchRepository(connector, index)
        else:
            raise ValueError(f"지원하지 않는 저장소 유형: {repo_type}")
