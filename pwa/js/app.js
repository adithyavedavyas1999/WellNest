/**
 * WellNest — main application controller.
 * Wires up search, map, detail panel, and PWA lifecycle hooks.
 */
(function () {
  'use strict';

  // --- DOM refs ---
  const $loading     = document.getElementById('loading-overlay');
  const $offline     = document.getElementById('offline-banner');
  const $searchInput = document.getElementById('search-input');
  const $searchClear = document.getElementById('search-clear');
  const $searchResults = document.getElementById('search-results');
  const $filterState = document.getElementById('filter-state');
  const $filterScore = document.getElementById('filter-score');
  const $btnLocate   = document.getElementById('btn-locate');
  const $btnInstall  = document.getElementById('btn-install');

  const $panel       = document.getElementById('school-panel');
  const $panelClose  = document.getElementById('panel-close');
  const $panelName   = document.getElementById('panel-school-name');
  const $panelLoc    = document.getElementById('panel-school-location');
  const $gaugeFill   = document.getElementById('gauge-fill');
  const $gaugeValue  = document.getElementById('gauge-value');
  const $scoreCards  = document.getElementById('score-cards');
  const $facts       = document.getElementById('school-facts');
  const $askInput    = document.getElementById('ask-input');
  const $askBtn      = document.getElementById('ask-btn');
  const $askAnswer   = document.getElementById('ask-answer');

  let deferredInstallPrompt = null;
  let activeSchoolId = null;

  // --- US state list for the filter dropdown ---
  const US_STATES = [
    ['AL','Alabama'],['AK','Alaska'],['AZ','Arizona'],['AR','Arkansas'],['CA','California'],
    ['CO','Colorado'],['CT','Connecticut'],['DE','Delaware'],['FL','Florida'],['GA','Georgia'],
    ['HI','Hawaii'],['ID','Idaho'],['IL','Illinois'],['IN','Indiana'],['IA','Iowa'],
    ['KS','Kansas'],['KY','Kentucky'],['LA','Louisiana'],['ME','Maine'],['MD','Maryland'],
    ['MA','Massachusetts'],['MI','Michigan'],['MN','Minnesota'],['MS','Mississippi'],
    ['MO','Missouri'],['MT','Montana'],['NE','Nebraska'],['NV','Nevada'],['NH','New Hampshire'],
    ['NJ','New Jersey'],['NM','New Mexico'],['NY','New York'],['NC','North Carolina'],
    ['ND','North Dakota'],['OH','Ohio'],['OK','Oklahoma'],['OR','Oregon'],['PA','Pennsylvania'],
    ['RI','Rhode Island'],['SC','South Carolina'],['SD','South Dakota'],['TN','Tennessee'],
    ['TX','Texas'],['UT','Utah'],['VT','Vermont'],['VA','Virginia'],['WA','Washington'],
    ['WV','West Virginia'],['WI','Wisconsin'],['WY','Wyoming'],['DC','District of Columbia'],
  ];

  // --- Init ---

  document.addEventListener('DOMContentLoaded', async () => {
    populateStateFilter();
    WellNestMap.initMap('map');
    setupSearch();
    setupFilters();
    setupPanel();
    setupPWA();
    setupOnlineStatus();
    handleHashRoute();
    window.addEventListener('hashchange', handleHashRoute);

    // initial school load
    try {
      const data = await WellNestAPI.fetchSchools({ limit: 500 });
      const schools = data.items || data;
      WellNestMap.loadSchoolMarkers(schools);
    } catch (err) {
      console.warn('Could not load initial schools:', err);
    }

    hideLoading();
  });

  function hideLoading() {
    $loading.classList.add('hidden');
    setTimeout(() => $loading.remove(), 500);
  }

  // --- State filter ---

  function populateStateFilter() {
    const frag = document.createDocumentFragment();
    US_STATES.forEach(([abbr, name]) => {
      const opt = document.createElement('option');
      opt.value = abbr;
      opt.textContent = name;
      frag.appendChild(opt);
    });
    $filterState.appendChild(frag);
  }

  // --- Search ---

  function setupSearch() {
    let debounceTimer = null;

    $searchInput.addEventListener('input', () => {
      const q = $searchInput.value.trim();
      $searchClear.hidden = q.length === 0;
      clearTimeout(debounceTimer);
      if (q.length < 2) {
        hideSearchResults();
        return;
      }
      debounceTimer = setTimeout(() => runSearch(q), 300);
    });

    $searchClear.addEventListener('click', () => {
      $searchInput.value = '';
      $searchClear.hidden = true;
      hideSearchResults();
      $searchInput.focus();
    });

    $searchInput.addEventListener('keydown', handleSearchKeyboard);
    document.addEventListener('click', (e) => {
      if (!e.target.closest('.search-container')) hideSearchResults();
    });
  }

  async function runSearch(query) {
    try {
      const data = await WellNestAPI.searchSchools(query);
      const results = data.items || data;
      renderSearchResults(results);
    } catch (err) {
      console.warn('Search failed:', err);
      hideSearchResults();
    }
  }

  function renderSearchResults(results) {
    if (!results.length) {
      $searchResults.innerHTML = '<li class="search-result-item" style="justify-content:center;color:var(--color-text-muted)">No schools found</li>';
      showSearchResults();
      return;
    }

    $searchResults.innerHTML = results.slice(0, 8).map((s, i) => {
      const cat = WellNestMap.scoreCategory(s.wellbeing_score);
      const color = WellNestMap.SCORE_COLORS[cat];
      const score = s.wellbeing_score != null ? Math.round(s.wellbeing_score) : '—';
      return `
        <li class="search-result-item" role="option" data-index="${i}" data-nces="${s.nces_id}">
          <span class="search-result-score" style="background:${color}">${score}</span>
          <div class="search-result-info">
            <div class="search-result-name">${escapeHtml(s.name)}</div>
            <div class="search-result-location">${escapeHtml(s.city || '')}, ${escapeHtml(s.state || '')}</div>
          </div>
        </li>
      `;
    }).join('');

    $searchResults.querySelectorAll('.search-result-item[data-nces]').forEach(el => {
      el.addEventListener('click', () => {
        const ncesId = el.dataset.nces;
        hideSearchResults();
        $searchInput.value = el.querySelector('.search-result-name').textContent;
        window.location.hash = `school/${ncesId}`;
      });
    });

    showSearchResults();
  }

  function showSearchResults() {
    $searchResults.hidden = false;
    $searchInput.setAttribute('aria-expanded', 'true');
  }

  function hideSearchResults() {
    $searchResults.hidden = true;
    $searchInput.setAttribute('aria-expanded', 'false');
  }

  function handleSearchKeyboard(e) {
    const items = $searchResults.querySelectorAll('.search-result-item[data-nces]');
    if (!items.length) return;

    let current = $searchResults.querySelector('[aria-selected="true"]');
    let idx = current ? parseInt(current.dataset.index, 10) : -1;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      idx = Math.min(idx + 1, items.length - 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      idx = Math.max(idx - 1, 0);
    } else if (e.key === 'Enter' && current) {
      e.preventDefault();
      current.click();
      return;
    } else if (e.key === 'Escape') {
      hideSearchResults();
      return;
    } else {
      return;
    }

    items.forEach(el => el.removeAttribute('aria-selected'));
    if (items[idx]) items[idx].setAttribute('aria-selected', 'true');
  }

  // --- Filters ---

  function setupFilters() {
    $filterState.addEventListener('change', applyFilters);
    $filterScore.addEventListener('change', applyFilters);
    $btnLocate.addEventListener('click', geolocateUser);
  }

  async function applyFilters() {
    const params = { limit: 500 };
    if ($filterState.value) params.state = $filterState.value;
    if ($filterScore.value) params.score_category = $filterScore.value;

    try {
      const data = await WellNestAPI.fetchSchools(params);
      const schools = data.items || data;
      WellNestMap.loadSchoolMarkers(schools);
      WellNestMap.fitToMarkers();
    } catch (err) {
      console.warn('Filter load failed:', err);
    }
  }

  function geolocateUser() {
    if (!navigator.geolocation) return;
    $btnLocate.disabled = true;

    navigator.geolocation.getCurrentPosition(
      async (pos) => {
        const { latitude, longitude } = pos.coords;
        WellNestMap.getMap().flyTo([latitude, longitude], 11, { duration: 1.5 });
        try {
          const data = await WellNestAPI.fetchSchools({ lat: latitude, lng: longitude, radius: 20, limit: 200 });
          const schools = data.items || data;
          WellNestMap.loadSchoolMarkers(schools);
        } catch (err) {
          console.warn('Nearby search failed:', err);
        }
        $btnLocate.disabled = false;
      },
      () => {
        $btnLocate.disabled = false;
        console.warn('Geolocation denied or unavailable');
      },
      { timeout: 10000 }
    );
  }

  // --- School detail panel ---

  function setupPanel() {
    $panelClose.addEventListener('click', closePanel);

    $askBtn.addEventListener('click', handleAsk);
    $askInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') handleAsk();
    });
  }

  function openPanel(school) {
    activeSchoolId = school.nces_id;
    $panelName.textContent = school.name;
    $panelLoc.textContent = [school.city, school.state].filter(Boolean).join(', ');

    renderGauge(school.wellbeing_score);
    renderScoreCards(school);
    renderFacts(school);

    $askAnswer.hidden = true;
    $askInput.value = '';

    $panel.classList.add('open');

    if (school.latitude && school.longitude) {
      WellNestMap.flyToSchool(school.latitude, school.longitude);
    }
  }

  function closePanel() {
    $panel.classList.remove('open');
    activeSchoolId = null;
    if (window.location.hash) history.replaceState(null, '', window.location.pathname);
  }

  function renderGauge(score) {
    const circumference = 2 * Math.PI * 52; // r=52
    const pct = score != null ? Math.max(0, Math.min(score, 100)) / 100 : 0;
    const offset = circumference * (1 - pct);

    const cat = WellNestMap.scoreCategory(score);
    const color = WellNestMap.SCORE_COLORS[cat];

    $gaugeFill.style.strokeDasharray = circumference;
    // force reflow so the transition actually plays
    $gaugeFill.style.strokeDashoffset = circumference;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        $gaugeFill.style.strokeDashoffset = offset;
        $gaugeFill.style.stroke = color;
      });
    });

    $gaugeValue.textContent = score != null ? Math.round(score) : '—';
    $gaugeValue.style.color = color;
  }

  function renderScoreCards(school) {
    const dimensions = [
      { key: 'academic_score',     label: 'Academic' },
      { key: 'health_score',       label: 'Health' },
      { key: 'economic_score',     label: 'Economic' },
      { key: 'environment_score',  label: 'Environment' },
    ];

    $scoreCards.innerHTML = dimensions.map(d => {
      const val = school[d.key];
      const cat = WellNestMap.scoreCategory(val);
      const color = WellNestMap.SCORE_COLORS[cat];
      const label = WellNestMap.scoreCategoryLabel(cat);
      const display = val != null ? Math.round(val) : '—';
      return `
        <div class="score-card">
          <div class="score-card-value" style="color:${color}">${display}</div>
          <div class="score-card-label">${d.label}</div>
          <span class="score-badge" style="background:${color}">${label}</span>
        </div>
      `;
    }).join('');
  }

  function renderFacts(school) {
    const facts = [
      ['Type', school.school_type],
      ['Grade Span', school.grade_span],
      ['Total Students', school.total_students != null ? school.total_students.toLocaleString() : null],
      ['Student–Teacher Ratio', school.student_teacher_ratio],
      ['Title I', school.title_i ? 'Yes' : 'No'],
      ['NCES ID', school.nces_id],
    ].filter(([, v]) => v != null);

    $facts.innerHTML = facts.map(([label, value]) => `
      <div class="fact-row">
        <span class="fact-label">${label}</span>
        <span class="fact-value">${value}</span>
      </div>
    `).join('');
  }

  async function handleAsk() {
    const q = $askInput.value.trim();
    if (!q || !activeSchoolId) return;

    $askBtn.disabled = true;
    $askAnswer.hidden = false;
    $askAnswer.textContent = 'Thinking…';

    try {
      const data = await WellNestAPI.askQuestion(`About school ${activeSchoolId}: ${q}`);
      $askAnswer.textContent = data.answer || data.response || 'No answer available.';
    } catch (err) {
      $askAnswer.textContent = 'Sorry, could not get an answer right now.';
    }
    $askBtn.disabled = false;
  }

  // --- Hash routing ---

  async function handleHashRoute() {
    const hash = window.location.hash.replace('#', '');
    if (!hash) return;

    const schoolMatch = hash.match(/^school\/(.+)$/);
    if (schoolMatch) {
      const ncesId = schoolMatch[1];
      try {
        const school = await WellNestAPI.getSchool(ncesId);
        openPanel(school);
      } catch (err) {
        console.warn('Could not load school', ncesId, err);
      }
    }
  }

  // --- PWA install prompt ---

  function setupPWA() {
    window.addEventListener('beforeinstallprompt', (e) => {
      e.preventDefault();
      deferredInstallPrompt = e;
      $btnInstall.hidden = false;
    });

    $btnInstall.addEventListener('click', async () => {
      if (!deferredInstallPrompt) return;
      deferredInstallPrompt.prompt();
      const { outcome } = await deferredInstallPrompt.userChoice;
      console.log('Install prompt outcome:', outcome);
      deferredInstallPrompt = null;
      $btnInstall.hidden = true;
    });

    window.addEventListener('appinstalled', () => {
      $btnInstall.hidden = true;
      deferredInstallPrompt = null;
    });
  }

  // --- Online/offline ---

  function setupOnlineStatus() {
    function update() {
      if (navigator.onLine) {
        $offline.classList.remove('visible');
      } else {
        $offline.removeAttribute('hidden');
        requestAnimationFrame(() => $offline.classList.add('visible'));
      }
    }
    window.addEventListener('online', update);
    window.addEventListener('offline', update);
    update();
  }

  // --- Util ---

  function escapeHtml(str) {
    const el = document.createElement('span');
    el.textContent = str || '';
    return el.innerHTML;
  }
})();
