// Service Worker - GS 트렌드 리포트 PWA
// Network-First 전략: 항상 최신 데이터 우선, 오프라인 시 캐시 사용

const CACHE_NAME = 'gs-report-v1';
const STATIC_ASSETS = [
  // 핵심 HTML 페이지
  './index.html',
  './weekly-compare.html',
  './gs-closing-report.html',
  './gs-report.html',
  './gs-sales-report.html',
  './gs-strategy-report.html',
  './booking-trend.html',
  './otb.html',
  './monthly.html',
  './ytd.html',

  // 외부 라이브러리 (CDN)
  'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js',
  'https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700;800;900&family=Nanum+Pen+Script&family=JetBrains+Mono:wght@400;500;700&display=swap',
  'https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.min.css'
];

// Service Worker 설치 (정적 자원 캐싱)
self.addEventListener('install', event => {
  console.log('[SW] Install 시작');
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('[SW] 정적 자원 캐싱 시작');
      return cache.addAll(STATIC_ASSETS).catch(err => {
        console.warn('[SW] 일부 자원 캐싱 실패:', err);
        // 모든 자원이 성공해야만 설치되는 것을 방지
        return Promise.resolve();
      });
    })
  );
  self.skipWaiting(); // 즉시 활성화
});

// Service Worker 활성화 (이전 캐시 정리)
self.addEventListener('activate', event => {
  console.log('[SW] Activate 시작');
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames
          .filter(cacheName => cacheName !== CACHE_NAME)
          .map(cacheName => {
            console.log('[SW] 이전 캐시 삭제:', cacheName);
            return caches.delete(cacheName);
          })
      );
    })
  );
  self.clients.claim(); // 모든 클라이언트 제어
});

// Fetch 이벤트 - Network-First 전략
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // 1. GET 요청만 처리
  if (request.method !== 'GET') {
    return;
  }

  // 2. 로컬 호스트/같은 도메인 요청
  if (request.mode === 'navigate') {
    // HTML 네비게이션: Network First
    event.respondWith(
      fetch(request)
        .then(response => {
          // 성공하면 캐시 업데이트하고 응답 반환
          if (response.ok) {
            const responseClone = response.clone();
            caches.open(CACHE_NAME).then(cache => {
              cache.put(request, responseClone);
            });
          }
          return response;
        })
        .catch(() => {
          // 네트워크 실패 시 캐시에서 반환
          return caches.match(request).then(cached => {
            if (cached) {
              console.log('[SW] 캐시에서 반환:', request.url);
              return cached;
            }
            // 캐시도 없으면 오프라인 페이지 반환
            return caches.match('./index.html');
          });
        })
    );
    return;
  }

  // 3. 정적 자원 (CSS, JS, 폰트): Cache-First
  if (
    request.url.includes('.css') ||
    request.url.includes('.js') ||
    request.url.includes('fonts') ||
    request.url.includes('.woff') ||
    request.url.includes('.woff2')
  ) {
    event.respondWith(
      caches.match(request).then(cached => {
        if (cached) {
          // 백그라운드에서 캐시 업데이트
          fetch(request).then(response => {
            if (response.ok) {
              caches.open(CACHE_NAME).then(cache => {
                cache.put(request, response.clone());
              });
            }
          }).catch(() => {});
          return cached;
        }
        // 캐시 없으면 네트워크에서 가져오기
        return fetch(request)
          .then(response => {
            if (response.ok) {
              const responseClone = response.clone();
              caches.open(CACHE_NAME).then(cache => {
                cache.put(request, responseClone);
              });
            }
            return response;
          })
          .catch(() => {
            // 실패해도 무시 (필수 아님)
            return new Response('리소스를 로드할 수 없습니다.', {
              status: 503,
              statusText: 'Service Unavailable'
            });
          });
      })
    );
    return;
  }

  // 4. JSON 데이터 API: Network-First (항상 최신 데이터)
  if (request.url.includes('.json') || request.url.includes('/api/')) {
    event.respondWith(
      fetch(request)
        .then(response => {
          if (response.ok) {
            const responseClone = response.clone();
            caches.open(CACHE_NAME).then(cache => {
              cache.put(request, responseClone);
            });
          }
          return response;
        })
        .catch(() => {
          // 네트워크 실패 시 캐시 사용 (최후의 수단)
          return caches.match(request).then(cached => {
            if (cached) {
              console.log('[SW] JSON 데이터 캐시 반환:', request.url);
              return cached;
            }
            return new Response(JSON.stringify({ error: 'No data available' }), {
              status: 503,
              headers: { 'Content-Type': 'application/json' }
            });
          });
        })
    );
    return;
  }

  // 5. 기타 요청: Network-First
  event.respondWith(
    fetch(request)
      .then(response => {
        if (response.ok) {
          const responseClone = response.clone();
          caches.open(CACHE_NAME).then(cache => {
            cache.put(request, responseClone);
          });
        }
        return response;
      })
      .catch(() => {
        return caches.match(request).then(cached => {
          return cached || new Response('오프라인 상태입니다.', { status: 503 });
        });
      })
  );
});

// Background Sync (옵션): 오프라인에서 캐시 업데이트 요청
self.addEventListener('sync', event => {
  if (event.tag === 'sync-data') {
    event.waitUntil(
      fetch('./manifest.json')
        .then(() => console.log('[SW] 백그라운드 동기화 성공'))
        .catch(() => console.log('[SW] 백그라운드 동기화 실패'))
    );
  }
});

// Push Notification (옵션)
self.addEventListener('push', event => {
  if (event.data) {
    const data = event.data.json();
    const options = {
      body: data.body || 'GS 트렌드 리포트 업데이트',
      icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 192 192"><defs><style>.icon-bg{fill:%231a1d23}.icon-text{fill:%23c9a063;font-family:sans-serif;font-size:96px;font-weight:700;text-anchor:middle;dominant-baseline:central}</style></defs><rect class="icon-bg" width="192" height="192"/><circle cx="96" cy="96" r="85" fill="%23252932"/><text class="icon-text" x="96" y="100">GS</text></svg>',
      badge: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96"><rect fill="%23c9a063" width="96" height="96"/></svg>',
      tag: 'gs-report-notification',
      requireInteraction: false
    };
    event.waitUntil(self.registration.showNotification('GS 트렌드 리포트', options));
  }
});

// Notification Click Handler
self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.matchAll({ type: 'window' }).then(clientList => {
      // 이미 열려 있는 창이 있으면 포커스
      for (let client of clientList) {
        if (client.url === './' && 'focus' in client) {
          return client.focus();
        }
      }
      // 없으면 새 창 열기
      if (clients.openWindow) {
        return clients.openWindow('./index.html');
      }
    })
  );
});

console.log('[SW] Service Worker 로드 완료');
