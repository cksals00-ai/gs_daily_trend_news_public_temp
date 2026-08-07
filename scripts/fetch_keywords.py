#!/usr/bin/env python3
"""
fetch_keywords.py — 키워드·수요 레이더 수집기
==============================================
SONO 관련 브랜드·경쟁사·테마·예약채널 키워드의 '보도량'을 네이버 뉴스 검색 API로 세어
docs/data/keyword_radar.json 으로 저장한다. 대시보드 ga-analytics.html 의
renderKeywordRadar() 가 이 파일을 읽어 그린다.

설계는 keywordradar.md(주간 키워드 레이더) 규칙을 따른다:
  - 응답을 그대로 세지 않는다. <b> 제거 → 키워드가 '문자 그대로' 든 기사만 → 띄어쓰기 변형 허용.
  - 최신순 페이지를 14일 경계에 닿을 때까지 스캔(최대 10페이지=1,000건).
  - cur=이번 7일, prev=그 앞 7일, top=이번 주 상위 3건.
  - prev=0 이면 비율을 내지 않고 '신규'(prev=0 그대로 저장, 화면이 처리).
  - 조회 실패는 0으로 채우지 않는다 — err 사유를 남긴다(0은 '측정된 0').

인증: 환경변수에서만 읽는다(문서/로그/JSON 어디에도 키를 적지 않는다).
  NCP_APIGW_API_KEY_ID, NCP_APIGW_API_KEY   (네이버 API HUB 검색 API)
일요일에만 도는 것을 권장(cron), 한 번 도는 데 키워드 1개당 최대 10콜.
의존: 표준 라이브러리만 사용(urllib).
"""
import os, re, json, sys, time
import urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(REPO, "docs", "data", "keyword_radar.json")

ENDPOINT = "https://naverapihub.apigw.ntruss.com/search/v1/news"
KEY_ID = os.environ.get("NCP_APIGW_API_KEY_ID")
KEY = os.environ.get("NCP_APIGW_API_KEY")

# 6그룹은 너무 넓어 SONO 유입과 직접 관련된 4그룹만 센다(키워드 변경은 여기서).
GROUPS = [
    ("자사 브랜드·프로모션", ["소노 브랜드데이", "오션월드", "소노벨", "비발디파크"]),
    ("경쟁 브랜드", ["한화리조트", "하이원리조트", "휘닉스파크", "롯데호텔"]),
    ("예약 채널(OTA)", ["트립닷컴", "야놀자", "여기어때", "아고다"]),
    ("리조트·호캉스 테마", ["호캉스", "워터파크", "풀빌라", "한옥스테이"]),
]

TAG = re.compile(r"</?b>")
_ws = lambda s: re.sub(r"\s+", "", s or "")


def _strip(s):
    return TAG.sub("", s or "")


def _variants(kw):
    """띄어쓰기 변형: '한화리조트' ↔ '한화 리조트' 를 같은 것으로."""
    v = {kw, _ws(kw)}
    return {_ws(x) for x in v}


def _match(kw, title, desc):
    """제목·본문에 키워드가 문자 그대로(공백 제거 기준) 들어있는지."""
    hay = _ws(_strip(title)) + " " + _ws(_strip(desc))
    return any(v and v in hay for v in _variants(kw))


def _req(query, start):
    url = ENDPOINT + "?" + urllib.parse.urlencode(
        {"query": query, "display": 100, "start": start, "sort": "date"})
    req = urllib.request.Request(url, headers={
        "X-NCP-APIGW-API-KEY-ID": KEY_ID or "",
        "X-NCP-APIGW-API-KEY": KEY or "",
    })
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def _pubdate(item):
    # RFC1123: 'Mon, 04 Aug 2026 09:00:00 +0900'
    try:
        return datetime.strptime(item["pubDate"], "%a, %d %b %Y %H:%M:%S %z")
    except Exception:
        return None


def scan(kw, now):
    """14일 창을 최신순으로 스캔해 cur/prev/top 산출. 실패 시 err."""
    w0 = now - timedelta(days=7)      # 이번 주 시작
    w_1 = now - timedelta(days=14)    # 그 앞 주 시작
    cur, prev, top = 0, 0, []
    try:
        for pg in range(10):          # 최대 10페이지=1,000건
            start = 1 + pg * 100
            if start > 901:
                break
            data = _req(kw, start)
            items = data.get("items", [])
            if not items:
                break
            reached = False
            for it in items:
                dt = _pubdate(it)
                if dt is None:
                    continue
                if dt < w_1:
                    reached = True
                    break
                if not _match(kw, it.get("title"), it.get("description")):
                    continue
                if dt >= w0:
                    cur += 1
                    if len(top) < 3:
                        top.append({"t": _strip(it.get("title")), "u": it.get("originallink") or it.get("link")})
                elif dt >= w_1:
                    prev += 1
            if reached:
                break
            time.sleep(0.1)
        return {"n": kw, "cur": cur, "prev": prev, "top": top}
    except Exception as e:
        return {"n": kw, "cur": None, "prev": None, "top": [], "err": str(e)[:120]}


def main():
    if not KEY_ID or not KEY:
        print("⚠ NCP_APIGW_API_KEY_ID/KEY 환경변수가 없어 수집을 건너뜁니다 — 기존 keyword_radar.json 유지")
        return 0
    now = datetime.now(KST)
    out = {
        "status": "live-news",
        "updated": now.strftime("%Y-%m-%d"),
        "weekFrom": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
        "weekTo": now.strftime("%Y-%m-%d"),
        "note": "네이버 뉴스 보도량 · 관심의 선행지표. 0은 '측정된 0', 실패는 err로 남김.",
        "groups": [],
    }
    calls = 0
    for gname, kws in GROUPS:
        rows = []
        for kw in kws:
            r = scan(kw, now)
            calls += 1
            rows.append(r)
            print(f"  · {gname} / {kw}: cur={r.get('cur')} prev={r.get('prev')}")
        out["groups"].append({"name": gname, "kw": rows})
    out["calls"] = calls
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"저장 완료 → {OUT} ({calls} calls)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
