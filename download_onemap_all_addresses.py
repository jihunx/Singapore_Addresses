#!/usr/bin/env python3
"""
OneMap 검색 API를 이용해 싱가포르 주소 + GPS 좌표를 수집하는 스크립트.

수집 전략:
1) 우편번호 앞 2자리(00~99)를 searchVal로 조회
2) 응답 결과 중 POSTAL이 해당 prefix로 시작하는 데이터만 채택
3) 중복 제거 후 CSV로 저장

주의:
- API 정책/약관/호출 제한은 사용자 환경에서 반드시 확인하세요.
- 대량 수집 시 시간이 오래 걸릴 수 있습니다.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
OUTPUT_FIELDS = [
    "SEARCHVAL",
    "BLK_NO",
    "ROAD_NAME",
    "BUILDING",
    "ADDRESS",
    "POSTAL",
    "X",
    "Y",
    "LATITUDE",
    "LONGITUDE",
]


@dataclass
class Config:
    api_token: str
    query_mode: str
    seed_queries: list[str]
    output_csv: Path
    checkpoint_json: Path
    timeout_sec: int
    pause_sec: float
    start_prefix: int
    end_prefix: int
    max_pages_per_query: int
    max_query_len: int


def build_session(api_token: str) -> requests.Session:
    session = requests.Session()
    token = api_token.strip()
    if token.lower().startswith("bearer "):
        session.headers.update({"Authorization": token})
    else:
        session.headers.update({"Authorization": f"Bearer {token}"})

    retries = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_page(session: requests.Session, prefix: str, page_num: int, timeout_sec: int) -> dict:
    params = {
        "searchVal": prefix,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": page_num,
    }
    response = session.get(BASE_URL, params=params, timeout=timeout_sec)
    response.raise_for_status()
    data = response.json()
    if "error" in data and data.get("error"):
        raise RuntimeError(f"API 오류(prefix={prefix}, page={page_num}): {data.get('error')}")
    return data


def normalize_record(raw: dict) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key in OUTPUT_FIELDS:
        normalized[key] = str(raw.get(key, "") or "").strip()

    # 일부 응답은 LONGTITUDE 오타 필드를 사용하므로 보정
    if not normalized["LONGITUDE"]:
        normalized["LONGITUDE"] = str(raw.get("LONGTITUDE", "") or "").strip()
    return normalized


def is_valid_postal_for_prefix(postal: str, prefix: str) -> bool:
    return len(postal) == 6 and postal.isdigit() and postal.startswith(prefix)


def is_valid_postal(postal: str) -> bool:
    return len(postal) == 6 and postal.isdigit()


def make_unique_key(rec: dict[str, str]) -> str:
    # 동일 주소/좌표가 중복 반환되는 케이스를 제거하기 위한 키
    return "|".join(
        [
            rec["POSTAL"],
            rec["ADDRESS"],
            rec["BLK_NO"],
            rec["ROAD_NAME"],
            rec["LATITUDE"],
            rec["LONGITUDE"],
        ]
    )


def load_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {"completed_prefixes": [], "records": []}

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    completed = data.get("completed_prefixes", [])
    records = data.get("records", [])
    if not isinstance(completed, list) or not isinstance(records, list):
        raise ValueError(f"체크포인트 형식 오류: {path}")

    return {"completed_prefixes": completed, "records": records}


def save_checkpoint(path: Path, completed_prefixes: Iterable[str], records: list[dict[str, str]]) -> None:
    payload = {
        "completed_prefixes": list(completed_prefixes),
        "records": records,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def build_queries(config: Config) -> list[str]:
    if config.seed_queries:
        return config.seed_queries
    if config.query_mode == "alpha":
        return [chr(code) for code in range(ord("a"), ord("z") + 1)]
    return [f"{n:02d}" for n in range(config.start_prefix, config.end_prefix + 1)]


def split_charset(query_mode: str) -> list[str]:
    if query_mode == "alpha":
        return [str(i) for i in range(10)] + [chr(code) for code in range(ord("a"), ord("z") + 1)]
    return [str(i) for i in range(10)]


def discover_leaf_queries(
    session: requests.Session,
    config: Config,
    seed_queries: list[str],
) -> tuple[list[tuple[str, dict]], list[str]]:
    leaves: list[tuple[str, dict]] = []
    warnings: list[str] = []
    queue = deque(seed_queries)
    charset = split_charset(config.query_mode)

    while queue:
        query = queue.popleft()
        page1 = fetch_page(session, query, 1, config.timeout_sec)
        total_pages = int(page1.get("totalNumPages", 0) or 0)

        if total_pages == 0:
            leaves.append((query, page1))
            continue

        if total_pages <= config.max_pages_per_query:
            leaves.append((query, page1))
            continue

        if len(query) >= config.max_query_len:
            warnings.append(
                f"query={query} 는 {total_pages}페이지이지만 최대 길이({config.max_query_len}) 도달로 {config.max_pages_per_query}페이지까지만 수집"
            )
            leaves.append((query, page1))
            continue

        for ch in charset:
            queue.append(f"{query}{ch}")

    return leaves, warnings


def collect_all(config: Config) -> None:
    session = build_session(config.api_token)

    cp = load_checkpoint(config.checkpoint_json)
    completed_prefixes = set(cp["completed_prefixes"])
    records = [normalize_record(r) for r in cp["records"]]

    unique_map: dict[str, dict[str, str]] = {}
    for rec in records:
        unique_map[make_unique_key(rec)] = rec

    seed_queries = build_queries(config)
    leaf_queries, warnings = discover_leaf_queries(session, config, seed_queries)
    total_queries = len(leaf_queries)
    print(f"[INFO] 수집 대상 쿼리 수: {total_queries}")
    if warnings:
        print(f"[WARN] 분할 한계 경고 {len(warnings)}건")
        for msg in warnings[:10]:
            print(f"       {msg}")
        if len(warnings) > 10:
            print(f"       ... 외 {len(warnings) - 10}건")

    for idx, (query, page1) in enumerate(leaf_queries, start=1):
        if query in completed_prefixes:
            print(f"[SKIP] {query} ({idx}/{total_queries}) 이미 완료")
            continue

        total_pages = int(page1.get("totalNumPages", 0) or 0)
        pages_to_fetch = min(total_pages, config.max_pages_per_query)
        print(f"[RUN ] {query} ({idx}/{total_queries}) 수집 시작, 페이지 {pages_to_fetch}/{total_pages}")

        if total_pages == 0:
            completed_prefixes.add(query)
            save_checkpoint(config.checkpoint_json, sorted(completed_prefixes), list(unique_map.values()))
            print(f"[DONE] {query} 결과 없음")
            continue

        for page in range(1, pages_to_fetch + 1):
            data = page1 if page == 1 else fetch_page(session, query, page, config.timeout_sec)
            results = data.get("results", [])

            for raw in results:
                rec = normalize_record(raw)
                if config.query_mode == "postal-prefix":
                    if not is_valid_postal_for_prefix(rec["POSTAL"], query):
                        continue
                else:
                    # alpha 모드는 포털 검색 결과에서 NIL/비정상 우편번호를 제거한다.
                    if not is_valid_postal(rec["POSTAL"]):
                        continue
                unique_map[make_unique_key(rec)] = rec

            if page % 50 == 0 or page == pages_to_fetch:
                print(f"       page {page}/{pages_to_fetch}, 누적 고유 레코드={len(unique_map)}")

            if config.pause_sec > 0:
                time.sleep(config.pause_sec)

        completed_prefixes.add(query)
        save_checkpoint(config.checkpoint_json, sorted(completed_prefixes), list(unique_map.values()))
        print(f"[DONE] {query} 완료, 누적 고유 레코드={len(unique_map)}")

    final_rows = sorted(
        unique_map.values(),
        key=lambda r: (
            r["POSTAL"],
            r["ROAD_NAME"],
            r["BLK_NO"],
            r["ADDRESS"],
        ),
    )
    write_csv(config.output_csv, final_rows)
    print(f"\n완료: {config.output_csv}")
    print(f"총 고유 주소 수: {len(final_rows)}")
    print(f"체크포인트: {config.checkpoint_json}")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="OneMap 주소/GPS 전체 수집기")
    parser.add_argument(
        "--api-token",
        default=os.environ.get("ONEMAP_API_TOKEN", ""),
        help="OneMap API 토큰 (미지정 시 ONEMAP_API_TOKEN 환경변수 사용)",
    )
    parser.add_argument(
        "--query-mode",
        choices=("alpha", "postal-prefix"),
        default="alpha",
        help="수집 쿼리 방식: alpha(권장) 또는 postal-prefix",
    )
    parser.add_argument(
        "--seed-queries",
        default="",
        help="직접 수집할 쿼리 목록(콤마 구분). 예: a,b,c 또는 06,07",
    )
    parser.add_argument(
        "--max-pages-per-query",
        type=int,
        default=100,
        help="쿼리당 최대 수집 페이지 수 (OneMap pageNum 제한 대응)",
    )
    parser.add_argument(
        "--max-query-len",
        type=int,
        default=3,
        help="자동 분할 시 쿼리 최대 길이",
    )
    parser.add_argument(
        "--output-csv",
        default="onemap_all_addresses.csv",
        help="최종 CSV 출력 경로",
    )
    parser.add_argument(
        "--checkpoint-json",
        default="onemap_checkpoint.json",
        help="중간 진행상태 저장 파일 경로",
    )
    parser.add_argument("--timeout-sec", type=int, default=20, help="요청 타임아웃(초)")
    parser.add_argument(
        "--pause-sec",
        type=float,
        default=0.05,
        help="요청 간 대기 시간(초). 서버 부하 완화를 위해 0 이상 권장",
    )
    parser.add_argument("--start-prefix", type=int, default=0, help="시작 우편 prefix(0~99)")
    parser.add_argument("--end-prefix", type=int, default=99, help="종료 우편 prefix(0~99)")
    args = parser.parse_args()

    if not (0 <= args.start_prefix <= 99 and 0 <= args.end_prefix <= 99):
        raise ValueError("start-prefix/end-prefix는 0~99 범위여야 합니다.")
    if not str(args.api_token).strip():
        raise ValueError(
            "OneMap API 토큰이 필요합니다. --api-token 또는 ONEMAP_API_TOKEN 환경변수를 설정하세요."
        )
    if args.start_prefix > args.end_prefix:
        raise ValueError("start-prefix는 end-prefix보다 작거나 같아야 합니다.")
    if args.timeout_sec <= 0:
        raise ValueError("timeout-sec는 1 이상이어야 합니다.")
    if args.pause_sec < 0:
        raise ValueError("pause-sec는 0 이상이어야 합니다.")
    if args.max_pages_per_query <= 0:
        raise ValueError("max-pages-per-query는 1 이상이어야 합니다.")
    if args.max_query_len <= 0:
        raise ValueError("max-query-len은 1 이상이어야 합니다.")

    seed_queries = [q.strip() for q in str(args.seed_queries).split(",") if q.strip()]

    return Config(
        api_token=str(args.api_token).strip(),
        query_mode=args.query_mode,
        seed_queries=seed_queries,
        output_csv=Path(args.output_csv),
        checkpoint_json=Path(args.checkpoint_json),
        timeout_sec=args.timeout_sec,
        pause_sec=args.pause_sec,
        start_prefix=args.start_prefix,
        end_prefix=args.end_prefix,
        max_pages_per_query=args.max_pages_per_query,
        max_query_len=args.max_query_len,
    )


def main() -> None:
    config = parse_args()
    collect_all(config)


if __name__ == "__main__":
    main()
