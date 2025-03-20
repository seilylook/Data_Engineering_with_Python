from pathlib import Path
from src.services.data_generation import DataGenerator, DatasetConfig
from src.services.data_extraction import collect_all_issues
from src.database.repository import (
    RepositoryFactory,
    PostgreSQLRepository,
    ElasticsearchRepository,
)
from src.database.scf_repository import SeeClickFixRepository
import logging
import sys
import pandas as pd
from typing import Dict, List, Tuple, Optional


def setup_logging() -> None:
    """기본 로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def check_db_connections() -> Dict[str, bool]:
    """데이터베이스 연결 확인

    Returns:
        Dict[str, bool]: 데이터베이스 종류(PostgreSQL | Elasticsearch) type
    """
    connection_status = {}

    # Check PostgreSQL connection
    try:
        pg_repo = RepositoryFactory.create("postgresql")
        connection_status["postgresql"] = pg_repo.check_connection()
    except Exception as e:
        logging.warning(f"PostgreSQL 데이터 베이스 연결 중 에러 발생...{e}")
        connection_status["postgresql"] = False

    # Check Elasticsearch connection
    try:
        es_repo = RepositoryFactory.create("elasticsearch")
        connection_status["elasticsearch"] = es_repo.check_connection()
    except Exception as e:
        logging.warning(f"Elasticsearch 데이터 베이스 연결 중 에러 발생...{e}")
        connection_status["elasticsearch"] = False

    return connection_status


def ensure_data_exists(record_count: int = 1000) -> Path:
    """프로그램 시작 시 생성한 데이터 존재하는지 체크

    Args:
        record_count (int, optional): 존재하지 않을 시 생성할 데이터셋의 Row 수 Defaults to 1000.

    Returns:
        Path: CSV 데이터 경로
    """
    output_path = Path("./data/raw")
    data_file = output_path / "test_data.csv"

    if not data_file.exists():
        logging.info(f"{output_path}에 데이터셋이 존재하지 않습니다...")
        try:
            output_path.mkdir(parents=True, exist_ok=True)
            generator = DataGenerator(DatasetConfig(record_count=record_count))
            generator.create_sample_dataset()

            logging.info(f"CSV 데이터셋이 생성되었습니다: {data_file}")
        except Exception as e:
            logging.error(f"CSV 데이터셋을 생성도중 에러가 발생했습니다: {e}")
            raise
    else:
        logging.info(f"데이터셋이 이미 존재합니다: {data_file}")

    return data_file


def save_data_to_postgresql(csv_path: Path) -> int:
    """PostgreSQL에 CSV 데이터 저장

    Args:
        csv_path (Path): CSV 파일 경로

    Returns:
        int: 저장된 레코드 수
    """
    try:
        df = pd.read_csv(csv_path)
        logging.info(f"PostgreSQL: {len(df)}개 레코드를 {csv_path}에서 읽었습니다")
        records = df.to_dict(orient="records")

        pg_repo = RepositoryFactory.create("postgresql")
        saved_count = pg_repo.bulk_save(records)
        logging.info(f"PostgreSQL에 {saved_count}개 레코드 저장 완료")

        return saved_count
    except Exception as e:
        logging.error(f"PostgreSQL 데이터 저장 중 오류: {e}")
        raise


def save_data_to_elasticsearch(csv_path: Path) -> int:
    """Elasticsearch에 CSV 데이터 저장

    Args:
        csv_path (Path): CSV 파일 경로

    Returns:
        int: 저장된 레코드 수
    """
    try:
        df = pd.read_csv(csv_path)
        logging.info(f"Elasticsearch: {len(df)}개 레코드를 {csv_path}에서 읽었습니다")
        records = df.to_dict(orient="records")

        es_repo = RepositoryFactory.create("elasticsearch")
        saved_count = es_repo.bulk_save(records)
        logging.info(f"Elasticsearch에 {saved_count}개 레코드 저장 완료")

        return saved_count
    except Exception as e:
        logging.error(f"Elasticsearch 데이터 저장 중 오류: {e}")
        raise


def check_postgresql_data() -> List[Dict]:
    """PostgreSQL에서 데이터 조회

    Returns:
        List[Dict]: 조회된 데이터 목록
    """
    try:
        pg_repo = PostgreSQLRepository()
        results = pg_repo.get_all(limit=5)  # 5개만 샘플로 조회
        logging.info(f"PostgreSQL에서 {len(results)}개 레코드 조회 완료")
        return results
    except Exception as e:
        logging.error(f"PostgreSQL 데이터 조회 중 오류: {e}")
        return []


def check_elasticsearch_data() -> List[Dict]:
    """Elasticsearch에서 데이터 조회

    Returns:
        List[Dict]: 조회된 데이터 목록
    """
    try:
        es_repo = ElasticsearchRepository()
        results = es_repo.get_all(limit=5)  # 5개만 샘플로 조회
        logging.info(f"Elasticsearch에서 {len(results)}개 레코드 조회 완료")
        return results
    except Exception as e:
        logging.error(f"Elasticsearch 데이터 조회 중 오류: {e}")
        return []


def main() -> None:
    """Application entry point that generates fake test data."""
    setup_logging()
    # logging.info("데이터 생성 및 저장 프로세스 시작")

    # # 1. Sample data 존재 체크
    # try:
    #     csv_path = ensure_data_exists()
    # except Exception as e:
    #     logging.error(f"데이터셋이 존재하지 않습니다: {e}")
    #     return

    # # 2. 데이터베이스 연결 확인
    # connection_status = check_db_connections()
    # for db_type, status in connection_status.items():
    #     status_text = "연결됨" if status else "연결 안됨"
    #     logging.info(f"{db_type.capitalize()} 상태: {status_text}")

    # # 3. PostgreSQL에 데이터 저장
    # if connection_status.get("postgresql", False):
    #     try:
    #         pg_saved = save_data_to_postgresql(csv_path)
    #         logging.info(f"PostgreSQL에 {pg_saved}개 레코드 저장됨")
    #     except Exception as e:
    #         logging.error(f"PostgreSQL에 저장 실패: {e}")
    # else:
    #     logging.warning("PostgreSQL이 연결되어 있지 않습니다")

    # # 4. Elasticsearch에 데이터 저장
    # if connection_status.get("elasticsearch", False):
    #     try:
    #         es_saved = save_data_to_elasticsearch(csv_path)
    #         logging.info(f"Elasticsearch에 {es_saved}개 레코드 저장됨")
    #     except Exception as e:
    #         logging.error(f"Elasticsearch에 저장 실패: {e}")
    # else:
    #     logging.warning("Elasticsearch가 연결되어 있지 않습니다")

    # # 5. 데이터베이스에 데이터 저장되었는지 확인
    # if connection_status.get("postgresql", False):
    #     pg_data = check_postgresql_data()
    #     if pg_data:
    #         logging.info(f"PostgreSQL 데이터 확인 (샘플 5개):")
    #         for i, record in enumerate(pg_data[:5], 1):
    #             logging.info(f"  레코드 {i}: {record}")

    # if connection_status.get("elasticsearch", False):
    #     es_data = check_elasticsearch_data()
    #     if es_data:
    #         logging.info(f"Elasticsearch 데이터 확인 (샘플 5개):")
    #         for i, record in enumerate(es_data[:5], 1):
    #             logging.info(f"  레코드 {i}: {record}")

    # logging.info("데이터 생성 및 저장 프로세스 완료")

    logging.info("========================")
    logging.info("SeeClickFix 데이터 수집 시작")
    logging.info("========================")

    # 저장소 생성
    repository = SeeClickFixRepository(index="scf")

    # 데이터 수집 및 저장 실행
    processed_count = collect_all_issues(repository)

    logging.info(f"처리 완료: {processed_count}개 이슈")


if __name__ == "__main__":
    main()
