from typing import Dict, List, Any, Optional
import logging
from src.database.repository import ElasticsearchRepository, ElasticsearchConnector

logger = logging.getLogger(__name__)


class SeeClickFixRepository(ElasticsearchRepository):
    """SeeClickFix 데이터를 위한 Elasticsearch 저장소"""

    def __init__(
        self, connector: Optional[ElasticsearchConnector] = None, index: str = "scf"
    ):
        """초기화

        Args:
            connector (Optional[ElasticsearchConnector], optional): Elasticsearch 연결 객체. Defaults to None.
            index (str, optional): 사용할 index 이름. Defaults to 'scf'.
        """
        super().__init__(connector, index)

    def setup_index_mapping(self) -> bool:
        """index mapping에 필요한 설정

        Returns:
            bool: 성공 여부
        """
        client = self.connector.get_client()
        if not client:
            self.logger.warning("Elasticsearch 클라이언트가 초기화되지 않았습니다.")
            return False

        # scf index가 없을 경우
        if not client.indices.exists(index=self.index):
            self.logger.info("SCF index가 존재하지 않아. 새롭게 만들겠습니다.")

            mapping = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "summary": {"type": "text"},
                        "description": {"type": "text"},
                        "status": {"type": "keyword"},
                        "lat": {"type": "float"},
                        "lng": {"type": "float"},
                        "coords": {"type": "geo_point"},
                        "address": {"type": "text"},
                        "rating": {"type": "integer"},
                        "comment_count": {"type": "integer"},
                        "view_count": {"type": "integer"},
                        "reporter": {"type": "keyword"},
                        "created_at": {"type": "date"},
                        "opendate": {"type": "date"},
                    }
                }
            }

            try:
                client.indices.create(index=self.index, body=mapping)
                self.logger.info(f"인덱스 {self.index} 생성 및 매핑 설정 완료")
                return True
            except Exception as e:
                self.logger.error(f"인덱스 생성 실패: {e}")
                return False

        return True

    def save_issue(self, issue: Dict[str, Any]) -> str:
        """개별 SeeClickFix 이슈 저장

        Args:
            issue (Dict[str, Any]): 저장할 이슈 데이터 딕션너리

        Returns:
            str: 저장된 이슈의 ID
        """
        issue_id = str(issue.get("id", ""))
        if not issue_id:
            self.logger.warning("Issue ID가 없습니다. 저장을 건너뜁니다.")

        client = self.connector.get_client()
        if not client:
            return None

        try:
            response = client.index(
                index=self.index, id=issue_id, document=issue, refresh=True
            )
            return response["_id"]
        except Exception as e:
            self.logger.warning(f"Issue 저장 실패 (ID: {issue_id}: {e})")
            return None

    def bulk_save_issues(
        self, issues: List[Dict[str, Any]], chunk_size: int = 1000
    ) -> int:
        """SeeClickFix 이슈 대량 저장

        Args:
            issues (List[Dict[str, Any]]): 저장할 이슈 데이터 딕션너리 목록
            chunk_size (int, optional): 자르는 기준이 되는 사이즈 Defaults to 1000.

        Returns:
            int: 성공적으로 저장된 Issue 수
        """
        if not issues:
            return 0

        # 작은 청크로 분할하지 않고 한 번에 더 많은 문서 처리
        client = self.connector.get_client()
        if not client:
            return 0

        operations = []
        for issue in issues:
            issue_id = str(issue.get("id", ""))
            if not issue_id:
                continue

            operations.append({"index": {"_index": self.index, "_id": issue_id}})
            operations.append(issue)

        if not operations:
            return 0

        # 벌크 크기 최적화
        total_saved = 0
        for i in range(0, len(operations), chunk_size * 2):
            chunk = operations[i : i + chunk_size * 2]
            response = client.bulk(
                operations=chunk, refresh=False
            )  # refresh=False로 속도 향상

            if response.get("errors", False):
                failed = sum(
                    1 for item in response["items"] if item["index"].get("error")
                )
                chunk_docs = len(chunk) // 2
                self.logger.warning(f"{failed}개 이슈 저장 실패 (총 {chunk_docs}개 중)")
                total_saved += chunk_docs - failed
            else:
                total_saved += len(chunk) // 2

        # 모든 처리가 끝난 후 한 번만 refresh
        client.indices.refresh(index=self.index)
        return total_saved

    def count_issues(self) -> int:
        """저장된 이슈 개수 반환

        Returns:
            int: 저장된 이슈 개수 반환
        """
        client = self.connector.get_client()
        if not client:
            return 0

        try:
            response = client.count(index=self.index)
            return response["count"]
        except Exception as e:
            self.logger.error(f"이슈 수 조회 실패: {e}")
            return 0
