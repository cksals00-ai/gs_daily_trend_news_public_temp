"""
GS 채널 판매 데이터 모델 — 회원번호/명 기반 실적관리 확장 대비

이 모듈은 dataclass 기반 스키마 정의입니다.
실제 DB 구현 시 SQLAlchemy/Django ORM 등으로 전환 가능하도록 설계.
"""

from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from typing import Optional
from enum import Enum


# ─── Enums ────────────────────────────────────────────────────

class GSChannel(str, Enum):
    INBOUND = 'Inbound'
    OTA     = 'OTA'
    G_OTA   = 'G-OTA'


class Division(str, Enum):
    VIVALDI       = '비발디파크'
    KOREA_SOUTH   = '한국남부'
    KOREA_CENTRAL = '한국중부'
    ASIA_PACIFIC  = '아시아퍼시픽'
    ALL           = '전사'


class PromotionStatus(str, Enum):
    PLANNED    = 'planned'
    ACTIVE     = 'active'
    COMPLETED  = 'completed'
    CANCELLED  = 'cancelled'


# ─── 기획전(Promotion) ───────────────────────────────────────

@dataclass
class Promotion:
    """기획전 마스터 테이블"""
    id: Optional[int] = None
    division: str = ''          # 사업본부 (구분)
    site: str = ''              # 사업장 (거제, 비발디 등)
    channel: str = ''           # 채널명 (원본 텍스트)
    gs_channel: str = ''        # GS채널 분류 (Inbound/OTA/G-OTA)
    sale_start: Optional[date] = None
    sale_end: Optional[date] = None
    stay_start: Optional[date] = None
    stay_end: Optional[date] = None
    branch: Optional[str] = None        # 영업장
    product_name: Optional[str] = None  # 상품명
    product: Optional[str] = None       # 상품 상세
    exposure: Optional[str] = None      # 노출영역
    note: Optional[str] = None          # 비고
    status: str = PromotionStatus.PLANNED.value
    source_sheet: str = 'DATA'  # 원본 시트명
    source_row: Optional[int] = None    # 원본 행 번호

    def is_active_on(self, check_date: date) -> bool:
        if self.sale_start and self.sale_end:
            return self.sale_start <= check_date <= self.sale_end
        return False

    def to_dict(self) -> dict:
        return asdict(self)


# ─── 인플루언서(Influencer) ──────────────────────────────────

@dataclass
class InfluencerPromotion:
    """인플루언서 기획전 (25/26 인플루언서 시트 대응)"""
    id: Optional[int] = None
    year: int = 2026
    category: str = ''          # 구분 (OTA 등)
    sale_period: str = ''       # 판매기간 텍스트
    stay_period: str = ''       # 투숙기간 텍스트
    site: str = ''              # 사업장
    influencer: str = ''        # 인플루언서/채널명
    product: str = ''           # 상품
    note: str = ''              # 기타
    channel_rns: Optional[float] = None    # 채널 판매량 RNs
    channel_rev: Optional[float] = None    # 채널 판매량 REV
    cancel_rate: Optional[float] = None    # 취소율
    otb_rns: Optional[float] = None        # OTB RNs
    otb_rev: Optional[float] = None        # OTB REV
    package_no: Optional[str] = None       # 패키지번호 (26년)
    remark: Optional[str] = None           # 비고


# ─── 회원 실적 관리 (확장용) ──────────────────────────────────

@dataclass
class Member:
    """회원 마스터 — 향후 실적관리 확장용"""
    member_id: str = ''         # 회원번호
    member_name: str = ''       # 회원명
    site: Optional[str] = None  # 소속 사업장
    department: Optional[str] = None
    created_at: Optional[datetime] = None


@dataclass
class MemberPerformance:
    """회원별 실적 (기획전 단위)"""
    id: Optional[int] = None
    member_id: str = ''         # FK → Member.member_id
    promotion_id: Optional[int] = None  # FK → Promotion.id
    period_start: Optional[date] = None
    period_end: Optional[date] = None
    rns_count: int = 0          # Room Night Sales
    revenue: float = 0.0        # 매출
    cancel_count: int = 0       # 취소 건수
    otb_rns: int = 0            # On-The-Books RNs
    otb_revenue: float = 0.0
    note: Optional[str] = None


# ─── 연간 플랜 ────────────────────────────────────────────────

@dataclass
class AnnualPlan:
    """26년 연간PLAN 시트 대응"""
    platform: str = ''          # 카카오메이커스, 프리즘, 놀유니버스 등
    site: str = ''
    month: int = 0              # 3~12
    schedule: str = ''          # 일정 텍스트 (예: "3/30~4/6")


# ─── DB 스키마 DDL (참고용) ───────────────────────────────────

SQL_SCHEMA = """
-- 기획전 마스터
CREATE TABLE IF NOT EXISTS promotions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    division        TEXT NOT NULL,
    site            TEXT NOT NULL,
    channel         TEXT NOT NULL,
    gs_channel      TEXT NOT NULL CHECK(gs_channel IN ('Inbound','OTA','G-OTA')),
    sale_start      DATE,
    sale_end        DATE,
    stay_start      DATE,
    stay_end        DATE,
    branch          TEXT,
    product_name    TEXT,
    product         TEXT,
    exposure        TEXT,
    note            TEXT,
    status          TEXT DEFAULT 'planned',
    source_sheet    TEXT DEFAULT 'DATA',
    source_row      INTEGER,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_promo_site ON promotions(site);
CREATE INDEX idx_promo_gs_channel ON promotions(gs_channel);
CREATE INDEX idx_promo_sale_period ON promotions(sale_start, sale_end);

-- 인플루언서 기획전
CREATE TABLE IF NOT EXISTS influencer_promotions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year            INTEGER NOT NULL,
    category        TEXT,
    sale_period     TEXT,
    stay_period     TEXT,
    site            TEXT NOT NULL,
    influencer      TEXT NOT NULL,
    product         TEXT,
    note            TEXT,
    channel_rns     REAL,
    channel_rev     REAL,
    cancel_rate     REAL,
    otb_rns         REAL,
    otb_rev         REAL,
    package_no      TEXT,
    remark          TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 회원 마스터 (확장용)
CREATE TABLE IF NOT EXISTS members (
    member_id       TEXT PRIMARY KEY,
    member_name     TEXT NOT NULL,
    site            TEXT,
    department      TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 회원별 실적
CREATE TABLE IF NOT EXISTS member_performances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id       TEXT NOT NULL REFERENCES members(member_id),
    promotion_id    INTEGER REFERENCES promotions(id),
    period_start    DATE,
    period_end      DATE,
    rns_count       INTEGER DEFAULT 0,
    revenue         REAL DEFAULT 0.0,
    cancel_count    INTEGER DEFAULT 0,
    otb_rns         INTEGER DEFAULT 0,
    otb_revenue     REAL DEFAULT 0.0,
    note            TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_mp_member ON member_performances(member_id);
CREATE INDEX idx_mp_promo ON member_performances(promotion_id);

-- 연간 플랜
CREATE TABLE IF NOT EXISTS annual_plans (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    platform        TEXT NOT NULL,
    site            TEXT NOT NULL,
    month           INTEGER CHECK(month BETWEEN 1 AND 12),
    schedule        TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


if __name__ == '__main__':
    print("=== SQL Schema ===")
    print(SQL_SCHEMA)
    print("\n=== Dataclass Examples ===")
    p = Promotion(
        division='한국중부', site='단양', channel='카카오 메이커스',
        gs_channel='Inbound', sale_start=date(2026, 4, 1), sale_end=date(2026, 4, 7)
    )
    print(f"Promotion: {p.to_dict()}")
    print(f"Is active on 2026-04-05? {p.is_active_on(date(2026, 4, 5))}")
