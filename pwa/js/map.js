/**
 * WellNest map utilities — Leaflet wrapper with clustering,
 * color-coded markers, county overlays, and a custom legend.
 */
const WellNestMap = (() => {
  let map = null;
  let markerCluster = null;
  let countyLayer = null;
  let legendControl = null;

  const TILE_URL = 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png';
  const TILE_ATTR =
    '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> contributors &copy; <a href="https://carto.com/">CARTO</a>';

  const US_CENTER = [39.5, -98.35];
  const US_ZOOM = 4;

  const SCORE_COLORS = {
    critical: '#C73E1D',
    atRisk:   '#F18F01',
    moderate: '#2E86AB',
    thriving: '#3BB273',
  };

  function scoreCategory(score) {
    if (score == null) return 'moderate';
    if (score <= 25) return 'critical';
    if (score <= 50) return 'atRisk';
    if (score <= 75) return 'moderate';
    return 'thriving';
  }

  function scoreCategoryLabel(cat) {
    const map = { critical: 'Critical', atRisk: 'At Risk', moderate: 'Moderate', thriving: 'Thriving' };
    return map[cat] || cat;
  }

  function markerRadius(zoom) {
    if (zoom >= 14) return 8;
    if (zoom >= 10) return 6;
    if (zoom >= 7)  return 5;
    return 4;
  }

  // --- public ---

  function initMap(elementId = 'map') {
    map = L.map(elementId, {
      center: US_CENTER,
      zoom: US_ZOOM,
      zoomControl: true,
      scrollWheelZoom: true,
      preferCanvas: true,
    });

    L.tileLayer(TILE_URL, {
      attribution: TILE_ATTR,
      subdomains: 'abcd',
      maxZoom: 19,
    }).addTo(map);

    markerCluster = L.markerClusterGroup({
      chunkedLoading: true,
      maxClusterRadius: 50,
      spiderfyOnMaxZoom: true,
      showCoverageOnHover: false,
      iconCreateFunction(cluster) {
        const count = cluster.getChildCount();
        let size = 'small';
        if (count > 100)      size = 'large';
        else if (count > 30)  size = 'medium';
        return L.divIcon({
          html: `<span>${count}</span>`,
          className: `marker-cluster marker-cluster-${size}`,
          iconSize: L.point(40, 40),
        });
      },
    });
    map.addLayer(markerCluster);

    addLegend();

    map.on('zoomend', () => {
      // nothing fancy yet, but hook is here for zoom-dependent behavior
    });

    return map;
  }

  function addLegend() {
    legendControl = L.control({ position: 'bottomright' });
    legendControl.onAdd = function () {
      const div = L.DomUtil.create('div', 'map-legend');
      div.innerHTML = `
        <h4>Wellbeing Score</h4>
        <div class="legend-item"><span class="legend-dot" style="background:${SCORE_COLORS.thriving}"></span> Thriving (76–100)</div>
        <div class="legend-item"><span class="legend-dot" style="background:${SCORE_COLORS.moderate}"></span> Moderate (51–75)</div>
        <div class="legend-item"><span class="legend-dot" style="background:${SCORE_COLORS.atRisk}"></span> At Risk (26–50)</div>
        <div class="legend-item"><span class="legend-dot" style="background:${SCORE_COLORS.critical}"></span> Critical (0–25)</div>
      `;
      return div;
    };
    legendControl.addTo(map);
  }

  function buildPopupHTML(school) {
    const cat = scoreCategory(school.wellbeing_score);
    const color = SCORE_COLORS[cat];
    const label = scoreCategoryLabel(cat);
    const score = school.wellbeing_score != null ? Math.round(school.wellbeing_score) : '—';

    return `
      <div class="popup-inner">
        <div class="popup-name">${escapeHtml(school.name)}</div>
        <div class="popup-location">${escapeHtml(school.city || '')}, ${escapeHtml(school.state || '')}</div>
        <span class="popup-score" style="background:${color}">${score} — ${label}</span>
        <a class="popup-link" href="#school/${school.nces_id}">View Details &rarr;</a>
      </div>
    `;
  }

  function loadSchoolMarkers(schools) {
    markerCluster.clearLayers();
    if (!schools || !schools.length) return;

    const zoom = map.getZoom();
    const radius = markerRadius(zoom);

    const markers = schools.map(s => {
      if (s.latitude == null || s.longitude == null) return null;
      const cat = scoreCategory(s.wellbeing_score);
      const color = SCORE_COLORS[cat];

      const marker = L.circleMarker([s.latitude, s.longitude], {
        radius,
        fillColor: color,
        color: '#fff',
        weight: 1.5,
        fillOpacity: 0.85,
      });

      marker.bindPopup(() => buildPopupHTML(s), { maxWidth: 280, className: 'wellnest-popup' });
      marker.schoolData = s;
      return marker;
    }).filter(Boolean);

    markerCluster.addLayers(markers);
  }

  function highlightCounty(geojson, fips) {
    if (countyLayer) {
      map.removeLayer(countyLayer);
      countyLayer = null;
    }
    if (!geojson) return;

    countyLayer = L.geoJSON(geojson, {
      style: {
        color: '#A23B72',
        weight: 2,
        fillColor: '#A23B72',
        fillOpacity: 0.1,
      },
      filter: (feature) => {
        if (!fips) return true;
        return feature.properties.FIPS === fips || feature.properties.fips === fips;
      },
    }).addTo(map);

    if (countyLayer.getBounds().isValid()) {
      map.fitBounds(countyLayer.getBounds(), { padding: [40, 40] });
    }
  }

  function clearCountyOverlay() {
    if (countyLayer) {
      map.removeLayer(countyLayer);
      countyLayer = null;
    }
  }

  function flyToSchool(lat, lng, zoom = 14) {
    if (!map) return;
    map.flyTo([lat, lng], zoom, { duration: 1.2 });
  }

  function fitToMarkers() {
    if (!markerCluster || !map) return;
    const bounds = markerCluster.getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [30, 30], maxZoom: 12 });
    }
  }

  function resetView() {
    if (!map) return;
    map.setView(US_CENTER, US_ZOOM);
  }

  function getMap() {
    return map;
  }

  function escapeHtml(str) {
    const el = document.createElement('span');
    el.textContent = str;
    return el.innerHTML;
  }

  return {
    initMap,
    loadSchoolMarkers,
    highlightCounty,
    clearCountyOverlay,
    flyToSchool,
    fitToMarkers,
    resetView,
    getMap,
    scoreCategory,
    scoreCategoryLabel,
    SCORE_COLORS,
  };
})();
