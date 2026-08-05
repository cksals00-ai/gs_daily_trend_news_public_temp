#!/usr/bin/env python3
"""
GA4 OAuth 리프레시 토큰 1회 발급 스크립트 (로컬 실행 전용)
=========================================================

관리자 권한 없이, GA 리포트 열람 권한이 있는 **본인 구글 계정**으로
GA4 Data API에 접근하기 위한 refresh token 을 발급한다.

준비:
  1) Google Cloud Console(프로젝트: GSN-Auth)에서
     "OAuth 2.0 클라이언트 ID"를 **데스크톱 앱** 유형으로 생성 → JSON 다운로드.
  2) OAuth 동의 화면에 범위 analytics.readonly 추가 + 본인 test user 등록
     (또는 앱 '게시(Production)' — 7일 만료 방지, 아래 주의 참고).

설치:
  pip install google-auth-oauthlib

실행 (본인 Mac 터미널에서):
  python scripts/ga4_oauth_setup.py ~/Downloads/client_secret_XXXX.json

  → 브라우저가 열리고 본인 계정으로 동의하면, 아래 3개 값을 출력한다:
      GA4_OAUTH_CLIENT_ID
      GA4_OAUTH_CLIENT_SECRET
      GA4_OAUTH_REFRESH_TOKEN
  → 이 3개를 GitHub 저장소 Secrets 에 등록하면 끝.

⚠ 주의(중요):
  OAuth 동의 화면이 '테스트(Testing)' 상태면 refresh token 이 7일 후 만료된다.
  파이프라인이 계속 돌게 하려면 동의 화면에서 **앱 게시(Production)** 를 눌러
  '프로덕션'으로 두는 걸 권장(미검증 경고는 본인 사용엔 무방).
"""

import json
import sys

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def main():
    if len(sys.argv) < 2:
        print("사용법: python scripts/ga4_oauth_setup.py <client_secret.json 경로>")
        return 1
    client_secret_file = sys.argv[1]

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("먼저 설치: pip install google-auth-oauthlib")
        return 1

    flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
    # offline + consent 강제 → refresh_token 확보
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

    if not creds.refresh_token:
        print("⚠ refresh_token 이 없습니다. 동의 화면에서 이미 승인된 앱이면 "
              "https://myaccount.google.com/permissions 에서 접근 권한을 제거 후 재실행하세요.")
        return 1

    print("\n" + "=" * 60)
    print("✅ 발급 완료 — 아래 3개를 GitHub Secrets 에 등록하세요")
    print("=" * 60)
    print(f"GA4_OAUTH_CLIENT_ID     = {creds.client_id}")
    print(f"GA4_OAUTH_CLIENT_SECRET = {creds.client_secret}")
    print(f"GA4_OAUTH_REFRESH_TOKEN = {creds.refresh_token}")
    print("=" * 60)

    # 참고용 로컬 저장(선택) — .gitignore 에 넣거나 등록 후 삭제 권장
    with open("ga4_oauth_token.json", "w", encoding="utf-8") as f:
        json.dump({
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "refresh_token": creds.refresh_token,
        }, f, indent=2)
    print("로컬 백업: ga4_oauth_token.json (등록 후 삭제 권장 · 절대 커밋 금지)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
