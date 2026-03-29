/*
 * WellNest Service Worker
 * Cache-first for static assets, network-first for API calls,
 * offline fallback for navigation requests.
 */

const CACHE_NAME = 'wellnest-v1';

const STATIC_ASSETS = [
  '/pwa/',
  '/pwa/index.html',
  '/pwa/css/styles.css',
  '/pwa/js/api.js',
  '/pwa/js/map.js',
  '/pwa/js/app.js',
  '/pwa/manifest.json',
  'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js',
];

const TILE_ORIGIN = 'basemaps.cartocdn.com';

// --- Install ---

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(STATIC_ASSETS).catch((err) => {
        console.warn('SW: some static assets failed to cache', err);
      });
    })
  );
  self.skipWaiting();
});

// --- Activate ---

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME)
          .map((key) => caches.delete(key))
      );
    })
  );
  self.clients.claim();
});

// --- Fetch ---

self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET requests
  if (request.method !== 'GET') return;

  // API calls: network-first with cache fallback
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(networkFirst(request));
    return;
  }

  // Map tiles: cache-first (tiles don't change often)
  if (url.hostname.includes(TILE_ORIGIN)) {
    event.respondWith(cacheFirst(request));
    return;
  }

  // Navigation requests: network-first, with offline fallback
  if (request.mode === 'navigate') {
    event.respondWith(
      networkFirst(request).catch(() => caches.match('/pwa/index.html'))
    );
    return;
  }

  // Everything else: cache-first
  event.respondWith(cacheFirst(request));
});

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;

  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('', { status: 503, statusText: 'Offline' });
  }
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response(JSON.stringify({ error: 'offline' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

// --- Background sync (for offline searches) ---

self.addEventListener('sync', (event) => {
  if (event.tag === 'offline-search') {
    event.waitUntil(replayOfflineSearches());
  }
});

async function replayOfflineSearches() {
  // Pending searches are stored in IndexedDB by the app.
  // When connectivity returns we re-execute them and notify the client.
  // Stubbed here — actual implementation depends on the IndexedDB schema.
  const clients = await self.clients.matchAll();
  clients.forEach((client) => {
    client.postMessage({ type: 'sync-complete', tag: 'offline-search' });
  });
}

// --- Messages from client ---

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
