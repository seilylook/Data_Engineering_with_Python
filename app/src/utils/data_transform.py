from typing import Dict, Any

REQUIRED_FIELDS = {
    "id",
    "summary",
    "description",
    "status",
    "lat",
    "lng",
    "address",
    "rating",
    "comment_count",
    "view_count",
    "created_at",
}


def transform_issue(issue: Dict[str, Any]) -> Dict[str, Any]:
    """
    API 응답에서 받은 이슈 데이터를 변환합니다.

    Args:
        issue: 원본 이슈 데이터 딕셔너리

    Returns:
        변환된 이슈 데이터 딕셔너리
    """
    # 딕셔너리 컴프리헨션으로 필요한 필드 추출 (None 기본값 활용)
    # 필요한 필드만 빠르게 추출
    transformed = {k: issue.get(k, "") for k in REQUIRED_FIELDS}

    # 숫자 필드 처리
    for num_field in ("lat", "lng", "rating", "comment_count", "view_count"):
        if num_field in transformed and transformed[num_field] == "":
            transformed[num_field] = 0

    # 좌표 정보 (조건 검사 최소화)
    if issue.get("lat") is not None and issue.get("lng") is not None:
        transformed["coords"] = f"{issue['lat']},{issue['lng']}"

    # 날짜 처리 (간단하게)
    created_at = issue.get("created_at", "")
    if created_at:
        transformed["opendate"] = created_at.split("T")[0]

    return transformed
