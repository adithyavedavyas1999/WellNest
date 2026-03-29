/**
 * WellNest API client.
 * Wraps fetch calls with caching, retries, and a consistent error shape.
 */
const WellNestAPI = (() => {
  const BASE_URL = window.WELLNEST_API_BASE || '/api';
  const MAX_RETRIES = 3;
  const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

  // --- helpers ---

  function cacheKey(path, params) {
    const qs = params ? '?' + new URLSearchParams(params).toString() : '';
    return `wn:${path}${qs}`;
  }

  function readCache(key) {
    try {
      const raw = sessionStorage.getItem(key);
      if (!raw) return null;
      const entry = JSON.parse(raw);
      if (Date.now() - entry.ts > CACHE_TTL_MS) {
        sessionStorage.removeItem(key);
        return null;
      }
      return entry.data;
    } catch {
      return null;
    }
  }

  function writeCache(key, data) {
    try {
      sessionStorage.setItem(key, JSON.stringify({ ts: Date.now(), data }));
    } catch {
      // quota exceeded — not the end of the world
    }
  }

  async function request(path, params, { retries = MAX_RETRIES, useCache = true } = {}) {
    const key = cacheKey(path, params);

    if (useCache) {
      const cached = readCache(key);
      if (cached) return cached;
    }

    const qs = params ? '?' + new URLSearchParams(params).toString() : '';
    const url = `${BASE_URL}${path}${qs}`;

    let lastError;
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const res = await fetch(url);
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          throw new APIError(res.status, body.detail || res.statusText, body);
        }
        const data = await res.json();
        if (useCache) writeCache(key, data);
        return data;
      } catch (err) {
        lastError = err;
        if (err instanceof APIError && err.status >= 400 && err.status < 500) {
          throw err; // no point retrying 4xx
        }
        if (attempt < retries - 1) {
          await sleep(300 * Math.pow(2, attempt));
        }
      }
    }
    throw lastError;
  }

  async function postRequest(path, body) {
    const url = `${BASE_URL}${path}`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      throw new APIError(res.status, data.detail || res.statusText, data);
    }
    return res.json();
  }

  function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
  }

  class APIError extends Error {
    constructor(status, message, body) {
      super(message);
      this.name = 'APIError';
      this.status = status;
      this.body = body;
    }
  }

  // --- public API ---

  return {
    APIError,

    fetchSchools(params) {
      return request('/schools', params);
    },

    searchSchools(query) {
      return request('/schools/search', { q: query });
    },

    getSchool(ncesId) {
      return request(`/schools/${encodeURIComponent(ncesId)}`);
    },

    getCounties(params) {
      return request('/counties', params);
    },

    getCounty(fips) {
      return request(`/counties/${encodeURIComponent(fips)}`);
    },

    getRankings(params) {
      return request('/rankings', params);
    },

    getResourceGaps(params) {
      return request('/resource-gaps', params);
    },

    askQuestion(question) {
      return postRequest('/ask', { question });
    },

    clearCache() {
      const toRemove = [];
      for (let i = 0; i < sessionStorage.length; i++) {
        const k = sessionStorage.key(i);
        if (k.startsWith('wn:')) toRemove.push(k);
      }
      toRemove.forEach(k => sessionStorage.removeItem(k));
    },
  };
})();
