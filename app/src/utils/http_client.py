import urllib3
import json
import certifi
from typing import Dict, Optional, Any
import logging
import time

logger = logging.getLogger(__name__)


# 동시에 여러 요청을 처리하므로 딜레이를 더 짧게 설정
# 또는 429 (Too Many Requests) 오류가 발생할 때만 지연을 두는 방식 적용
def fetch_page(
    base_url: str, params: Dict[str, Any], max_retries: int = 3
) -> Optional[Dict[str, Any]]:
    """특정 페이지 데이터를 가져온다.

    Args:
        base_url (str): API endpoint
        params (Dict[str, Any]): URL 쿼리 파라미터
        max_retries (int, optional): 최대 시도 횟수. Defaults to 3.

    Returns:
        Optional[Dict[str, Any]]: 응답 데이터 딕션너리 || 오류 시 None
    """
    http = urllib3.PoolManager(cert_reqs="CERT_REQUIRED", ca_certs=certifi.where())
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"

    retries = 0
    while retries < max_retries:
        try:
            response = http.request("GET", url)
            if response.status == 200:
                return json.loads(response.data.decode("utf-8"))
            elif response.status == 429:  # 요청 제한에 걸린 경우
                retries += 1
                logger.warning(
                    f"API 요청 제한 발생. {retries}/{max_retries} 재시도 중..."
                )
                time.sleep(2**retries)  # 지수 백오프 적용
                continue
            else:
                logger.error(
                    f"Error fetching page {params.get('page', 'unknown')}: Status code {response.status}"
                )
                return None
        except Exception as e:
            logger.error(f"Exception occurred: {e}")
            return None

    logger.error(f"최대 재시도 횟수 초과: {params.get('page', 'unknown')}")
    return None
