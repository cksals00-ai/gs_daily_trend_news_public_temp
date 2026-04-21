# 🚨 GitHub Pages 설정 확인 (1회 필수)

## 문제
캡처에서 보인 워크플로우 `pages-build-deployment`는 GitHub의 **기본 자동 배포**입니다.
우리가 만든 `🚀 Auto Build & Deploy` 워크플로우가 실행되려면, 
GitHub Pages 설정을 **`GitHub Actions`** 모드로 변경해야 합니다.

## 해결 방법 (1분 소요)

### 1단계: Settings 접속
```
https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/settings/pages
```

### 2단계: Build and deployment > Source 변경

현재 설정 (문제):
```
Source: Deploy from a branch    ← ❌
Branch: main / docs
```

변경할 설정:
```
Source: GitHub Actions    ← ✅
```

### 3단계: 저장
"Save" 버튼 클릭

## 변경 후

다음 푸시부터 우리가 만든 `🚀 Auto Build & Deploy` 워크플로우가 트리거됩니다:
- Power BI 자동 수집 ✅
- 뉴스 자동 수집 ✅
- GS Monitor 자동 수집 ✅
- 인사이트 자동 생성 ✅
- 빌드 + 배포 ✅

## 확인 방법

설정 변경 후 다음 푸시 시:
```
https://github.com/cksals00-ai/gs_daily_trend_news_public_temp/actions
```
→ 좌측 사이드바에 **"🚀 Auto Build & Deploy"** 워크플로우 표시 확인
