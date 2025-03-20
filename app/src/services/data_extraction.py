import concurrent.futures
from functools import partial
import time
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from src.utils.http_client import fetch_page
from src.utils.data_transform import transform_issue
from src.database.scf_repository import SeeClickFixRepository
from src.modules.progress_bar import print_progress_bar

# 로거 설정
logger = logging.getLogger(__name__)

# 데이터 저장 디렉토리
DATA_DIR = Path("data/raw/seeclickfix")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# SeeClickFix API 설정
BASE_URL = "https://seeclickfix.com/api/v2/issues"
PLACE_URL = "bernalillo-county"
PER_PAGE = 100
STATUS = "Archived"  # 또는 "open", "acknowledged", "closed" 등


def save_to_file(data: Dict[str, Any], page_num: int) -> None:
    """데이터를 로컬 파일로 저장합니다."""
    filename = DATA_DIR / f"seeclickfix_page_{page_num}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved page {page_num} to {filename}")


def process_page(
    page_num: int, base_url: str, params: Dict[str, Any], repo: SeeClickFixRepository
) -> int:
    """단일 페이지를 처리하는 함수"""
    page_params = params.copy()
    page_params["page"] = page_num

    page_data = fetch_page(base_url, page_params)
    if not page_data or "issues" not in page_data:
        logger.error(f"페이지 {page_num} 가져오기 실패")
        return 0

    # 로컬 파일에 원본 데이터 저장
    save_to_file(page_data, page_num)

    # 데이터 변환 및 저장
    issues = [transform_issue(issue) for issue in page_data.get("issues", [])]
    if issues:
        saved_count = repo.bulk_save_issues(issues)
        logger.info(f"저장 완료: {saved_count}/{len(issues)} 이슈 (페이지 {page_num})")
        return saved_count
    return 0


def collect_all_issues(repository: Optional[SeeClickFixRepository] = None) -> int:
    """모든 페이지의 데이터를 가져와 Elasticsearch에 저장합니다."""
    logger.info("Starting data collection from SeeClickFix API...")

    # 저장소 초기화
    repo = repository or SeeClickFixRepository()
    if not repo.check_connection():
        logger.error("Elasticsearch 연결 실패. 데이터 수집을 중단합니다.")
        return 0

    # 인덱스 매핑 설정
    if not repo.setup_index_mapping():
        logger.error("인덱스 매핑 설정 실패. 데이터 수집을 중단합니다.")
        return 0

    # 첫 페이지를 가져와 총 페이지 수 확인
    params = {"place_url": PLACE_URL, "per_page": PER_PAGE, "status": STATUS, "page": 1}

    first_page = fetch_page(BASE_URL, params)
    if not first_page or "metadata" not in first_page:
        logger.error("첫 번째 페이지 가져오기 실패 또는 응답 형식 오류")
        return 0

    pagination = first_page["metadata"]["pagination"]
    # 기본적으로 3273개의 pages가 존재한다. 하지만 이렇게 돌릴 경우 너무 시간이 오래 걸리기에 100정도로 낮춰서 테스트해본다.
    # total_pages = pagination["pages"]
    # logger.info(f"총 페이지: {total_pages}")

    total_pages = 50

    # 첫 페이지 처리
    save_to_file(first_page, 1)
    issues = [transform_issue(issue) for issue in first_page.get("issues", [])]
    total_processed = 0
    if issues:
        saved_count = repo.bulk_save_issues(issues)
        logger.info(f"첫 페이지 저장 완료: {saved_count}/{len(issues)} 이슈")
        total_processed += saved_count

    # 페이지 범위 설정 (첫 페이지는 이미 처리했으므로 2부터 시작)
    page_range = range(2, total_pages + 1)

    # 작업자 수 설정 (너무 많으면 API 제한에 걸릴 수 있음)
    max_workers = 5

    # 진행 상황 표시 초기화
    print_progress_bar(
        1,
        total_pages,
        prefix="데이터 수집:",
        suffix=f"1/{total_pages} 페이지",
        length=50,
    )

    # 처리된 페이지 수 카운터
    processed_pages = 1  # 첫 페이지는 이미 처리함

    # 부분 함수로 process_page 함수의 일부 인자 고정
    process_func = partial(process_page, base_url=BASE_URL, params=params, repo=repo)

    # ThreadPoolExecutor를 사용한 병렬 처리
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 페이지 번호를 작업으로 제출
        future_to_page = {
            executor.submit(process_func, page_num): page_num for page_num in page_range
        }

        # 완료된 작업 처리
        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                saved_count = future.result()
                total_processed += saved_count
            except Exception as e:
                logger.error(f"페이지 {page_num} 처리 중 오류: {e}")

            # 진행 상황 업데이트
            processed_pages += 1
            print_progress_bar(
                processed_pages,
                total_pages,
                prefix="데이터 수집:",
                suffix=f"{processed_pages}/{total_pages} 페이지",
                length=50,
            )

    # 최종 저장된 항목 수 확인
    final_count = repo.count_issues()
    logger.info(
        f"데이터 수집 완료. 총 {total_pages} 페이지, {total_processed} 이슈 처리됨"
    )
    logger.info(f"Elasticsearch에 저장된 총 이슈 수: {final_count}")

    return total_processed
