# API Guide

Complete reference for the WellNest REST API. The API serves child wellbeing data for 130,000+ US public schools. Interactive docs (Swagger UI) are available at `/docs` and ReDoc at `/redoc`.

## Base URL

```
Local:       http://localhost:8000/api
Production:  https://api.wellnest.chieac.org/api  (when deployed)
```

All endpoints are prefixed with `/api`.

## Authentication

The API uses header-based API key authentication. Include your key in the `X-API-Key` header:

```bash
curl -H "X-API-Key: your-api-key" http://localhost:8000/api/schools
```

**In development mode** (when `API_KEY` is not set in the environment), authentication is bypassed entirely. All endpoints are accessible without a key.

**The health endpoint** (`/api/health`) never requires authentication, so monitoring tools can hit it freely.

To get an API key, contact the project admin. Keys are simple bearer tokens -- no OAuth, no JWT. We'll switch to something more sophisticated if we ever need user-level permissions, but for now this is fine for our use case (a handful of internal consumers).

## Versioning

There's no version prefix in the URL right now (no `/v1/`). The API is pre-1.0 and evolving. If we ever need to make breaking changes, we'll add versioning at that point.

## Pagination

All list endpoints return paginated results in a consistent envelope:

```json
{
  "items": [...],
  "total": 130412,
  "page": 1,
  "per_page": 50,
  "pages": 2609
}
```

**Parameters:**

| Param | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `page` | int | 1 | >= 1 | Page number (1-indexed) |
| `per_page` | int | 50 | 1-200 | Items per page |

```bash
# page 3 with 25 items per page
curl "http://localhost:8000/api/schools?page=3&per_page=25"
```

## Common Filters

Several endpoints share these query parameters:

| Param | Type | Description |
|-------|------|-------------|
| `state` | string (2 chars) | Two-letter state code (e.g., `IL`, `CA`) |
| `score_below` | float (0-100) | Maximum composite score |
| `score_above` | float (0-100) | Minimum composite score |

## Endpoints

### Health Check

```
GET /api/health
```

No authentication required. Returns API status and database connectivity.

**Response:**

```json
{
  "status": "ok",
  "version": "0.1.0",
  "environment": "development",
  "database": "connected",
  "timestamp": "2024-11-15T14:30:00Z"
}
```

### Quick Stats

```
GET /api/stats
```

Aggregate numbers for the dashboard hero section.

**Response:**

```json
{
  "total_schools": 130412,
  "total_counties": 3221,
  "avg_score": 52.3,
  "min_score": 3.2,
  "max_score": 96.8
}
```

---

### Schools

#### List Schools

```
GET /api/schools
```

Paginated list of schools ordered by composite score (ascending -- worst first, because that's the most common use case for our NGO users).

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `state` | string | Filter by state code |
| `score_below` | float | Max composite score |
| `score_above` | float | Min composite score |
| `title_i` | bool | Filter to Title I schools only |
| `pillar` | string | Filter by pillar: `education`, `health`, `environment`, `safety` |
| `page` | int | Page number |
| `per_page` | int | Items per page |

**Example:**

```bash
# Illinois schools scoring below 30
curl "http://localhost:8000/api/schools?state=IL&score_below=30&per_page=10"
```

**Response:**

```json
{
  "items": [
    {
      "nces_id": "170993000943",
      "name": "Lincoln Elementary School",
      "city": "Chicago",
      "state": "IL",
      "composite_score": 24.7,
      "category": "critical",
      "enrollment": 342,
      "title_i": true,
      "location": {
        "lat": 41.8781,
        "lon": -87.6298
      }
    }
  ],
  "total": 847,
  "page": 1,
  "per_page": 10,
  "pages": 85
}
```

#### Get School Detail

```
GET /api/schools/{nces_id}
```

Full school profile with pillar breakdown, raw metrics, rankings, and YoY change.

**Path Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `nces_id` | string | 12-digit NCES school identifier |

**Example:**

```bash
curl http://localhost:8000/api/schools/170993000943
```

**Response:**

```json
{
  "nces_id": "170993000943",
  "name": "Lincoln Elementary School",
  "address": "1234 S State St",
  "city": "Chicago",
  "state": "IL",
  "zip_code": "60605",
  "county_fips": "17031",
  "county_name": "Cook County",
  "school_type": "Regular",
  "grade_range": "PK-5",
  "enrollment": 342,
  "title_i": true,
  "location": {
    "lat": 41.8781,
    "lon": -87.6298
  },
  "composite_score": 24.7,
  "category": "critical",
  "national_rank": 118432,
  "state_rank": 3241,
  "pillar_scores": [
    {
      "pillar": "education",
      "score": 31.2,
      "category": "at_risk",
      "national_percentile": 22
    },
    {
      "pillar": "health",
      "score": 18.5,
      "category": "critical",
      "national_percentile": 11
    },
    {
      "pillar": "environment",
      "score": 42.1,
      "category": "at_risk",
      "national_percentile": 38
    },
    {
      "pillar": "safety",
      "score": 15.3,
      "category": "critical",
      "national_percentile": 8
    }
  ],
  "metrics": {
    "math_proficiency": 22.5,
    "reading_proficiency": 28.1,
    "chronic_absenteeism_rate": 38.2,
    "student_teacher_ratio": 18.4,
    "poverty_rate": 42.1,
    "uninsured_children_rate": 8.3,
    "food_desert": true,
    "hpsa_score": 18,
    "aqi_avg": 47,
    "violent_crime_rate": 612.3,
    "social_vulnerability": 0.82
  },
  "score_change_1y": -3.2,
  "prediction": null,
  "updated_at": "2024-11-10T02:15:00Z"
}
```

#### Get School Predictions

```
GET /api/schools/{nces_id}/predictions
```

XGBoost prediction for next-year proficiency change.

**Response:**

```json
{
  "nces_id": "170993000943",
  "predicted_score_change": -4.2,
  "confidence_interval_low": -7.1,
  "confidence_interval_high": -1.3,
  "risk_flag": true,
  "top_contributing_factors": [
    "chronic_absenteeism_trend",
    "poverty_rate_change",
    "enrollment_decline"
  ],
  "model_version": "xgboost_v1.2",
  "predicted_at": "2024-11-01"
}
```

Returns 404 if no prediction exists for the school.

---

### Counties

#### List Counties

```
GET /api/counties
```

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `state` | string | Filter by state code |
| `score_below` | float | Max composite score |
| `score_above` | float | Min composite score |
| `min_schools` | int | Minimum school count in county |

**Example:**

```bash
curl "http://localhost:8000/api/counties?state=IL&min_schools=10"
```

**Response:**

```json
{
  "items": [
    {
      "fips": "17031",
      "name": "Cook County",
      "state": "IL",
      "composite_score": 41.8,
      "category": "at_risk",
      "school_count": 1247,
      "population": 5150233
    }
  ],
  "total": 84,
  "page": 1,
  "per_page": 50,
  "pages": 2
}
```

#### Get County Detail

```
GET /api/counties/{fips}
```

Full county profile with pillar scores, key metrics, and AI-generated brief.

**Response:**

```json
{
  "fips": "17031",
  "name": "Cook County",
  "state": "IL",
  "composite_score": 41.8,
  "category": "at_risk",
  "school_count": 1247,
  "population": 5150233,
  "centroid": {
    "lat": 41.8403,
    "lon": -87.8168
  },
  "education_score": 38.5,
  "health_score": 35.2,
  "environment_score": 52.1,
  "safety_score": 28.7,
  "avg_poverty_rate": 18.6,
  "avg_chronic_absenteeism": 22.3,
  "pct_title_i": 68.4,
  "ai_brief": "Cook County presents significant challenges for child wellbeing, with health and safety scores well below national averages. The county's 1,247 schools serve a diverse population of over 5 million, but 68% are Title I eligible...",
  "score_change_1y": -1.4,
  "updated_at": "2024-11-10T02:15:00Z"
}
```

#### List Schools in a County

```
GET /api/counties/{fips}/schools
```

All schools within a given county, ordered by composite score (ascending).

Standard pagination applies. Returns 404 if no schools exist in the county.

---

### Search

```
GET /api/search
```

Text search on school name, city, and state. Uses PostgreSQL ILIKE matching.

**Query Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `q` | string | Yes | Search query (2-200 chars) |
| `state` | string | No | Narrow results to a specific state |

**Example:**

```bash
curl "http://localhost:8000/api/search?q=Lincoln+Elementary&state=IL"
```

Returns the same `PaginatedResponse[SchoolSummary]` as the schools endpoint.

The search matches against school name, city, and state columns with OR logic, so "Springfield IL" will match schools in Springfield *or* any school in Illinois.

---

### Rankings

```
GET /api/rankings
```

National or state-level school rankings by composite score.

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `state` | string | If provided, returns state rankings instead of national |

**Response:**

```json
{
  "items": [
    {
      "rank": 1,
      "nces_id": "060198000123",
      "school_name": "Sunnydale Academy",
      "city": "Palo Alto",
      "state": "CA",
      "composite_score": 96.8,
      "category": "thriving",
      "score_change_1y": 2.1
    }
  ],
  "total": 130412,
  "page": 1,
  "per_page": 50,
  "pages": 2609
}
```

---

### Anomalies

```
GET /api/anomalies
```

Schools flagged by the anomaly detection pipeline (Isolation Forest + z-score).

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `state` | string | Filter by state |
| `anomaly_type` | string | `improvement` or `decline` |

**Response:**

```json
{
  "items": [
    {
      "nces_id": "170993001234",
      "school_name": "Washington High School",
      "state": "IL",
      "composite_score": 45.2,
      "score_change_1y": -18.3,
      "z_score": -3.4,
      "anomaly_type": "decline",
      "narrative": "Washington High School: Math proficiency decreased by 18.3 points YoY. Enrollment decreased 12%. Flagged by both Isolation Forest and z-score -- high confidence anomaly.",
      "detected_at": "2024-11-01T04:00:00Z"
    }
  ],
  "total": 4231,
  "page": 1,
  "per_page": 50,
  "pages": 85
}
```

---

### Predictions

```
GET /api/predictions
```

Batch access to ML predictions (XGBoost proficiency change predictor).

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `state` | string | Filter by state |
| `risk_only` | bool | Only show schools flagged as at-risk |

**Example:**

```bash
# at-risk schools in Illinois
curl "http://localhost:8000/api/predictions?state=IL&risk_only=true"
```

---

### Ask (RAG)

```
POST /api/ask
```

Natural language question answering over federal education and health policy documents.

**Request Body:**

```json
{
  "question": "What are the Title I funding requirements for schools with high poverty rates?",
  "max_sources": 3
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `question` | string | Yes | 5-1000 chars |
| `max_sources` | int | No | 1-10, default 3 |

**Response:**

```json
{
  "question": "What are the Title I funding requirements for schools with high poverty rates?",
  "answer": "Under Title I, Part A of ESSA, schools with poverty rates above 40% are eligible for schoolwide programs...",
  "sources": [
    {
      "document": "essa_title_i_guidance.pdf",
      "page": 12,
      "chunk_text": "Section 1114(b) allows schools with poverty rates of 40 percent or more to operate schoolwide programs...",
      "relevance_score": 0.89
    }
  ],
  "model": "gpt-4o-mini",
  "responded_at": "2024-11-15T14:30:00Z"
}
```

Returns 503 if the RAG pipeline is unavailable (missing OPENAI_API_KEY or FAISS index).

---

### Reports

```
GET /api/reports/{fips}/pdf
```

Download a PDF county wellbeing report. Reports are cached on disk and regenerated by the Dagster pipeline.

**Response:** Binary PDF file (`Content-Type: application/pdf`)

```bash
curl -o cook_county.pdf http://localhost:8000/api/reports/17031/pdf
```

---

## Error Handling

All errors return JSON with a `detail` field:

```json
{
  "detail": "School 999999999999 not found",
  "path": "/api/schools/999999999999",
  "timestamp": "2024-11-15T14:30:00Z"
}
```

**Status Codes:**

| Code | Meaning |
|------|---------|
| 200 | Success |
| 401 | Missing or invalid API key |
| 404 | Resource not found |
| 422 | Validation error (bad query params) |
| 429 | Rate limit exceeded |
| 500 | Internal server error |
| 503 | Service unavailable (RAG not loaded) |

The 422 response includes details about which parameter failed validation:

```json
{
  "detail": [
    {
      "loc": ["query", "per_page"],
      "msg": "Input should be less than or equal to 200",
      "type": "less_than_equal"
    }
  ]
}
```

## Rate Limiting

The API uses an in-memory token bucket rate limiter (per IP address).

- **Default:** 100 requests per 60 seconds
- **Burst:** Up to 100 requests immediately, then tokens refill at ~1.67/sec
- **Exempt paths:** `/api/health`, `/docs`, `/openapi.json`

Rate limit headers are included in every response:

```
X-RateLimit-Remaining: 87
X-RateLimit-Limit: 100
```

When rate limited, you'll get:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 2
Content-Type: application/json

{"detail": "Rate limit exceeded. Slow down."}
```

Wait for the `Retry-After` seconds before retrying.

## Code Examples

### Python

```python
import httpx

BASE = "http://localhost:8000/api"
HEADERS = {"X-API-Key": "your-key"}

# search for schools
resp = httpx.get(f"{BASE}/search", params={"q": "Lincoln Elementary", "state": "IL"}, headers=HEADERS)
schools = resp.json()["items"]

# get detail for first result
if schools:
    nces_id = schools[0]["nces_id"]
    detail = httpx.get(f"{BASE}/schools/{nces_id}", headers=HEADERS).json()
    print(f"{detail['name']}: {detail['composite_score']} ({detail['category']})")

# ask a policy question
resp = httpx.post(f"{BASE}/ask", json={"question": "What is Title I?"}, headers=HEADERS)
print(resp.json()["answer"])
```

### curl

```bash
# list critical schools in Texas
curl -s -H "X-API-Key: $WELLNEST_API_KEY" \
  "http://localhost:8000/api/schools?state=TX&score_below=25&per_page=5" | \
  python -m json.tool

# download county PDF
curl -H "X-API-Key: $WELLNEST_API_KEY" \
  -o harris_county.pdf \
  http://localhost:8000/api/reports/48201/pdf

# ask the RAG pipeline
curl -s -X POST -H "X-API-Key: $WELLNEST_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"question": "What does ESSA say about chronic absenteeism?"}' \
  http://localhost:8000/api/ask | python -m json.tool
```

### Pagination Loop (Python)

```python
import httpx

def fetch_all_schools(state: str, headers: dict) -> list[dict]:
    """Fetch all schools for a state, handling pagination."""
    all_items = []
    page = 1
    while True:
        resp = httpx.get(
            "http://localhost:8000/api/schools",
            params={"state": state, "page": page, "per_page": 200},
            headers=headers,
        )
        data = resp.json()
        all_items.extend(data["items"])
        if page >= data["pages"]:
            break
        page += 1
    return all_items
```

## Response Time

The API includes an `X-Response-Time` header on every response showing server-side processing time:

```
X-Response-Time: 12.3ms
```

Typical response times against a local Postgres:
- `/api/health`: < 5ms
- `/api/schools` (paginated): 10-30ms
- `/api/schools/{id}` (detail): 5-15ms
- `/api/search`: 20-50ms (depends on query)
- `/api/counties/{fips}`: 5-15ms
- `/api/ask` (RAG): 2-8 seconds (depends on OpenAI latency)
- `/api/reports/{fips}/pdf`: 100-500ms (first time, cached after)
