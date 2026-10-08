# 괌 채널 분석 규칙

버전: 2026-10-08-v1. 원본 Channel·Nationality·Total은 변경하지 않습니다. 분석용 필드만 추가합니다.

| 순서 | 조건 | 분석 채널 |
|---|---|---|
| 1 | 기존 원본 채널 또는 같은 예약의 엑셀 보완 채널 | 기존 분류 유지 |
| 2 | 기존 엑셀의 정확한 거래처·고객그룹 규칙에 일치 | 기존 분류 유지 |
| 3 | 상품명에 MILITARY | MILITARY |
| 4 | 상품명에 GUAM RESIDENT, RESIDENT TWILIGHT, GF RESIDENT, LOCAL GROUP, LOCAL GRP, LOCAL MEMBERSHIP, RES PROMO, US CITIZEN | LOCAL |
| 5 | 고객그룹 Local Clubs 또는 Local Membership(오탈자 Memebership 포함) | LOCAL |
| 6 | RITA TOURS / DAEKUN TOUR BOOKING KOREA | Other |
| 7 | SPORTS NIPPON SHIMBUNSHA (SPONICHI) | JP GROUP |
| 8 | 고객그룹 없음 + 원본 국적 KR/JP + 상품 FIT/PACK/PKG MEMBER/18H PRO | KR/JP 개인 분석 |
| 9 | 위 조건에 불일치 | 미분류 |

## 검증

2026-10-07 엑셀의 골프 예약 3,787건에서 채널을 가리고 대조했습니다. 기존 거래처 규칙으로 1,484건을 유지하고, 패턴으로 분류한 2,272건은 모두 기존 엑셀과 일치했습니다. 31건은 미확정으로 남겼습니다. 이 검증은 규칙을 만든 동일 자료에서의 대조이며, 과거 모든 기간의 정답을 입증하는 것은 아닙니다.

2026-10-08 현재 자료에는 기존 예약별 엑셀 보완을 우선 적용하고 남은 미분류만 분석합니다. 미분류가 147건에서 11건으로 줄었으며 총계 3,875건과 매출은 동일합니다.

상품명·고객그룹은 내보내기 시점의 명칭일 수 있습니다. 실제 2020년 자료에서도 '(2026)' 상품명이 나와 과거 이름이 보존됐다고 가정하지 않습니다. 원본 Type·Client Group ID를 보존하고, 상품 의미가 바뀌면 해당 규칙은 재검토해야 합니다.

## 전년비

현재 예약 / 전년 동월 최종 예약 실적의 비교입니다. 지난해 같은 조회일의 예약 속도 비교가 아닙니다. 취소 및 비골프 예약 제외, 같은 분석 규칙 사용, 미분류 별도 표시. 고정 엑셀 전년 수치는 참고 필드로만 보존하며 실제 원본이 있으면 이를 우선합니다.
