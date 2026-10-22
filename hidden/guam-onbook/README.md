# 괌 On Book — 비공개 보관 (2026-10-22)

대표 지시로 괌 대시보드를 웹에서 내렸다. GitHub Pages는 `docs/`만 게시하므로 이 폴더의 파일은 주소로 직접 들어가도 열리지 않는다.

- 보관 파일: guam-onbook.html · guam-onbook-guide.html · guam-onbook-risks.html · js/guam-onbook.js · data/guam_onbook.json
- 아웃바운드 대시보드(docs/overseas.html)의 「괌」 탭도 hidden 처리(마크업은 남김, 스크립트 오류 방지). 탭 안의 On Book 링크는 삭제.
- 다시 게시하려면: 위 파일을 원래 위치(docs/, docs/js/, docs/data/)로 되돌리고 overseas.html 괌 탭의 `hidden style="display:none"`을 지운다.
- 맥의 업데이트 도구(scripts/guam_onbook)는 docs/data/guam_onbook.json 을 다시 만든다. 다시 공개할 게 아니면 그 결과를 커밋하지 말 것.
