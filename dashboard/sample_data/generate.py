"""
Generate realistic sample data for the WellNest dashboard demo.

Run once:  python -m dashboard.sample_data.generate
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUT = Path(__file__).parent

# ── reference data ──────────────────────────────────────────────────────────

STATES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
}

STATE_LAT_LNG = {
    "AL": (32.8, -86.8),
    "AK": (64.2, -152.5),
    "AZ": (34.0, -111.1),
    "AR": (34.7, -92.4),
    "CA": (36.8, -119.4),
    "CO": (39.1, -105.4),
    "CT": (41.6, -72.7),
    "DE": (39.0, -75.5),
    "FL": (27.8, -81.8),
    "GA": (32.7, -83.5),
    "HI": (19.9, -155.6),
    "ID": (44.1, -114.7),
    "IL": (40.3, -89.0),
    "IN": (40.3, -86.1),
    "IA": (42.0, -93.2),
    "KS": (38.5, -98.8),
    "KY": (37.8, -84.3),
    "LA": (30.5, -91.2),
    "ME": (45.3, -69.4),
    "MD": (39.0, -76.6),
    "MA": (42.4, -71.4),
    "MI": (44.3, -85.6),
    "MN": (46.4, -94.6),
    "MS": (32.7, -89.7),
    "MO": (38.5, -92.3),
    "MT": (46.8, -110.4),
    "NE": (41.1, -98.3),
    "NV": (38.8, -116.4),
    "NH": (43.2, -71.6),
    "NJ": (40.1, -74.5),
    "NM": (34.3, -106.0),
    "NY": (43.0, -75.0),
    "NC": (35.6, -79.0),
    "ND": (47.5, -100.5),
    "OH": (40.4, -82.9),
    "OK": (35.0, -97.1),
    "OR": (43.8, -120.5),
    "PA": (41.2, -77.2),
    "RI": (41.6, -71.5),
    "SC": (34.0, -81.0),
    "SD": (43.9, -99.4),
    "TN": (35.5, -86.6),
    "TX": (31.0, -100.0),
    "UT": (39.3, -111.1),
    "VT": (44.0, -72.7),
    "VA": (37.4, -78.2),
    "WA": (47.7, -120.7),
    "WV": (38.6, -80.6),
    "WI": (43.8, -88.8),
    "WY": (43.1, -107.6),
}

# State-level bias: some states tend higher/lower (loosely inspired by real data)
STATE_BIAS = {
    "MA": 8,
    "CT": 7,
    "NJ": 6,
    "NH": 7,
    "VT": 6,
    "MN": 5,
    "VA": 4,
    "MD": 3,
    "CO": 4,
    "WA": 3,
    "HI": 3,
    "UT": 3,
    "IA": 2,
    "WI": 2,
    "PA": 1,
    "NY": 1,
    "IL": 0,
    "OR": 1,
    "CA": 0,
    "OH": -1,
    "MI": -2,
    "IN": -1,
    "FL": -1,
    "TX": -1,
    "GA": -2,
    "NC": -1,
    "MO": -2,
    "NV": -2,
    "AZ": -1,
    "KY": -3,
    "TN": -2,
    "SC": -3,
    "LA": -5,
    "AR": -4,
    "AL": -5,
    "MS": -7,
    "WV": -5,
    "NM": -4,
    "OK": -3,
}

SCHOOL_NAME_PREFIXES = [
    "Washington",
    "Lincoln",
    "Jefferson",
    "Franklin",
    "Roosevelt",
    "Martin Luther King Jr.",
    "Kennedy",
    "Adams",
    "Madison",
    "Jackson",
    "Monroe",
    "Hamilton",
    "Grant",
    "Wilson",
    "Edison",
    "Emerson",
    "Whitman",
    "Longfellow",
    "Hawthorne",
    "Twain",
    "Westside",
    "Eastside",
    "Northview",
    "Southfield",
    "Lakeview",
    "Riverside",
    "Oakwood",
    "Maple",
    "Cedar",
    "Pine",
    "Elm",
    "Birch",
    "Willow",
    "Magnolia",
    "Sunrise",
    "Sunset",
    "Meadow",
    "Valley",
    "Highland",
    "Hillcrest",
    "Ridgewood",
    "Fairview",
    "Pleasant",
    "Heritage",
    "Liberty",
    "Freedom",
    "Unity",
    "Harmony",
    "Discovery",
    "Innovation",
    "Achievement",
    "Excellence",
    "Horizon",
    "Pioneer",
    "Centennial",
    "Central",
    "Spring",
    "Autumn",
    "Greenfield",
    "Brookside",
]

SCHOOL_NAME_SUFFIXES = [
    "Elementary School",
    "Middle School",
    "High School",
    "Academy",
    "Preparatory School",
    "Magnet School",
    "Charter School",
    "Elementary",
    "Primary School",
    "Junior High School",
]

COUNTY_NAMES = [
    "Washington",
    "Jefferson",
    "Franklin",
    "Lincoln",
    "Madison",
    "Jackson",
    "Marion",
    "Clay",
    "Monroe",
    "Grant",
    "Hamilton",
    "Greene",
    "Union",
    "Warren",
    "Clark",
    "Montgomery",
    "Adams",
    "Carroll",
    "Henry",
    "Marshall",
    "Lawrence",
    "Morgan",
    "Perry",
    "Pike",
    "Shelby",
    "Sullivan",
    "Wayne",
    "Lake",
    "Orange",
    "Lee",
    "Scott",
    "Douglas",
    "Fulton",
    "Hancock",
    "Knox",
    "Logan",
    "Putnam",
    "Russell",
    "Randolph",
    "Crawford",
    "Fayette",
    "Delaware",
    "Harrison",
    "Howard",
    "Johnson",
    "Mercer",
    "Noble",
    "Owen",
    "Ripley",
    "Spencer",
    "Stark",
    "Taylor",
    "Wabash",
]

CITIES = [
    "Springfield",
    "Franklin",
    "Greenville",
    "Bristol",
    "Fairview",
    "Clinton",
    "Madison",
    "Georgetown",
    "Arlington",
    "Ashland",
    "Burlington",
    "Chester",
    "Dayton",
    "Easton",
    "Farmington",
    "Glen Cove",
    "Hartford",
    "Jamestown",
    "Kingston",
    "Lexington",
    "Manchester",
    "Newport",
    "Oxford",
    "Plymouth",
    "Richmond",
    "Salem",
    "Troy",
    "Vernon",
    "Weston",
    "York",
]

GAP_TYPES = ["healthcare", "food_access", "mental_health", "dental"]
ANOMALY_TYPES = ["improvement", "decline"]
FLAG_TYPES = ["missing_data", "outlier", "stale_source", "inconsistency"]


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _category(score: float) -> str:
    if score > 75:
        return "thriving"
    if score > 50:
        return "moderate"
    if score > 25:
        return "at_risk"
    return "critical"


def _school_name() -> str:
    return f"{random.choice(SCHOOL_NAME_PREFIXES)} {random.choice(SCHOOL_NAME_SUFFIXES)}"


def _write(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def generate() -> None:

    now = datetime.utcnow()
    updated_at = (now - timedelta(hours=2)).isoformat()

    # ── counties (4 per state = 200 counties) ───────────────────────────────
    counties: list[dict] = []
    county_idx = 0
    for abbr, _full_name in STATES.items():
        sfips = STATE_FIPS[abbr]
        lat0, lng0 = STATE_LAT_LNG[abbr]
        n_counties = 4
        for j in range(n_counties):
            cfips = f"{sfips}{(j + 1):03d}"
            cname = COUNTY_NAMES[county_idx % len(COUNTY_NAMES)]
            county_idx += 1
            bias = STATE_BIAS.get(abbr, 0)
            base = random.gauss(55 + bias, 14)
            comp = round(_clamp(base), 1)
            edu = round(_clamp(base + random.gauss(0, 8)), 1)
            hlt = round(_clamp(base + random.gauss(-2, 10)), 1)
            env = round(_clamp(base + random.gauss(1, 9)), 1)
            saf = round(_clamp(base + random.gauss(0, 7)), 1)
            pop = random.randint(8000, 950000)
            sc = random.randint(3, 80)
            counties.append(
                {
                    "fips": cfips,
                    "name": f"{cname} County",
                    "state": abbr,
                    "composite_score": comp,
                    "education_score": edu,
                    "health_score": hlt,
                    "environment_score": env,
                    "safety_score": saf,
                    "school_count": sc,
                    "population": pop,
                    "category": _category(comp),
                    "latitude": round(lat0 + random.uniform(-1.2, 1.2), 4),
                    "longitude": round(lng0 + random.uniform(-1.5, 1.5), 4),
                    "score_change_1y": round(random.gauss(0.5, 3.5), 1),
                }
            )

    _write(OUT / "county_summary.csv", counties)

    # ── schools (5 per county = 1000 schools) ───────────────────────────────
    schools: list[dict] = []
    scores: list[dict] = []
    school_nces_ids: list[str] = []

    for c in counties:
        n_schools = 5
        for _k in range(n_schools):
            nces = f"{c['fips']}{random.randint(1000, 9999):04d}"
            school_nces_ids.append(nces)
            name = _school_name()
            city = random.choice(CITIES)
            enrollment = random.randint(120, 2400)
            title_i = random.choice(["Yes", "No", "No", "No"])
            lat = c["latitude"] + random.uniform(-0.3, 0.3)
            lng = c["longitude"] + random.uniform(-0.3, 0.3)
            grade_range = random.choice(["PK-5", "K-5", "K-8", "6-8", "9-12", "K-12"])
            school_type = random.choice(["Regular", "Regular", "Regular", "Magnet", "Charter"])

            schools.append(
                {
                    "nces_id": nces,
                    "name": name,
                    "city": city,
                    "state": c["state"],
                    "county_fips": c["fips"],
                    "county_name": c["name"],
                    "school_type": school_type,
                    "grade_range": grade_range,
                    "enrollment": enrollment,
                    "title_i": title_i,
                    "latitude": round(lat, 5),
                    "longitude": round(lng, 5),
                }
            )

            bias = STATE_BIAS.get(c["state"], 0)
            base = random.gauss(55 + bias, 16)
            comp = round(_clamp(base), 1)
            edu = round(_clamp(base + random.gauss(0, 10)), 1)
            hlt = round(_clamp(base + random.gauss(-1, 10)), 1)
            env = round(_clamp(base + random.gauss(1, 9)), 1)
            saf = round(_clamp(base + random.gauss(0, 8)), 1)
            change = round(random.gauss(0.3, 4.0), 1)

            scores.append(
                {
                    "nces_id": nces,
                    "school_name": name,
                    "state": c["state"],
                    "county_fips": c["fips"],
                    "composite_score": comp,
                    "education_score": edu,
                    "health_score": hlt,
                    "environment_score": env,
                    "safety_score": saf,
                    "category": _category(comp),
                    "national_rank": 0,
                    "state_rank": 0,
                    "score_change_1y": change,
                    "updated_at": updated_at,
                }
            )

    # compute ranks
    scores.sort(key=lambda r: r["composite_score"], reverse=True)
    for i, row in enumerate(scores):
        row["national_rank"] = i + 1

    from collections import defaultdict

    state_groups: dict[str, list[dict]] = defaultdict(list)
    for row in scores:
        state_groups[row["state"]].append(row)
    for grp in state_groups.values():
        grp.sort(key=lambda r: r["composite_score"], reverse=True)
        for i, row in enumerate(grp):
            row["state_rank"] = i + 1

    _write(OUT / "school_profiles.csv", schools)
    _write(OUT / "child_wellbeing_scores.csv", scores)

    # ── trends (3 years) ────────────────────────────────────────────────────
    trends: list[dict] = []
    for sc in scores:
        for yr in [2022, 2023, 2024]:
            drift = (yr - 2024) * random.uniform(-2, 2)
            trends.append(
                {
                    "nces_id": sc["nces_id"],
                    "year": yr,
                    "state": sc["state"],
                    "county_fips": sc["county_fips"],
                    "composite_score": round(
                        _clamp(sc["composite_score"] + drift + random.gauss(0, 2)), 1
                    ),
                    "education_score": round(
                        _clamp(sc["education_score"] + drift + random.gauss(0, 3)), 1
                    ),
                    "health_score": round(
                        _clamp(sc["health_score"] + drift + random.gauss(0, 3)), 1
                    ),
                    "environment_score": round(
                        _clamp(sc["environment_score"] + drift + random.gauss(0, 2)), 1
                    ),
                    "safety_score": round(
                        _clamp(sc["safety_score"] + drift + random.gauss(0, 3)), 1
                    ),
                }
            )

    _write(OUT / "trend_metrics.csv", trends)

    # ── resource gaps (for ~30% of schools) ─────────────────────────────────
    gaps: list[dict] = []
    gap_schools = random.sample(scores, k=int(len(scores) * 0.3))
    for sc in gap_schools:
        n_gaps = random.randint(1, 3)
        for gt in random.sample(GAP_TYPES, k=min(n_gaps, len(GAP_TYPES))):
            gaps.append(
                {
                    "nces_id": sc["nces_id"],
                    "school_name": sc["school_name"],
                    "state": sc["state"],
                    "county_fips": sc["county_fips"],
                    "gap_type": gt,
                    "severity": round(random.uniform(15.0, 98.0), 1),
                    "composite_score": sc["composite_score"],
                    "description": f"Identified {gt.replace('_', ' ')} gap in service area",
                }
            )

    _write(OUT / "resource_gaps.csv", gaps)

    # ── anomalies (~5% of schools) ──────────────────────────────────────────
    anomalies: list[dict] = []
    anom_schools = random.sample(scores, k=int(len(scores) * 0.05))
    for sc in anom_schools:
        atype = random.choice(ANOMALY_TYPES)
        z = round(random.uniform(2.0, 4.5) * (1 if atype == "improvement" else -1), 2)
        change = round(random.uniform(5, 20) * (1 if atype == "improvement" else -1), 1)
        if atype == "improvement":
            narrative = (
                f"{sc['school_name']} in {sc['state']} showed a remarkable {abs(change)}-point "
                f"improvement in composite score, driven primarily by gains in education "
                f"and health metrics. This improvement stands out significantly from peer schools."
            )
        else:
            narrative = (
                f"{sc['school_name']} in {sc['state']} experienced a {abs(change)}-point "
                f"decline in composite score. Key contributing factors include decreased "
                f"environment and safety scores. Immediate attention is recommended."
            )
        anomalies.append(
            {
                "school_name": sc["school_name"],
                "state": sc["state"],
                "composite_score": sc["composite_score"],
                "score_change_1y": change,
                "z_score": z,
                "anomaly_type": atype,
                "narrative": narrative,
                "detected_at": (now - timedelta(days=random.randint(0, 14))).isoformat(),
            }
        )

    _write(OUT / "anomalies.csv", anomalies)

    # ── AI briefs (one per county) ──────────────────────────────────────────
    briefs: list[dict] = []
    for c in counties:
        cat = c["category"]
        if cat == "thriving":
            outlook = "continues to demonstrate strong outcomes across all pillars"
        elif cat == "moderate":
            outlook = "shows solid fundamentals with room for targeted improvement"
        elif cat == "at_risk":
            outlook = "faces notable challenges that require coordinated intervention"
        else:
            outlook = "is in critical need of immediate, multi-sector support"

        brief_text = (
            f"## {c['name']}, {STATES.get(c['state'], c['state'])}\n\n"
            f"**Overall Score: {c['composite_score']}/100 ({cat.replace('_', ' ').title()})**\n\n"
            f"{c['name']} {outlook}. With a population of {c['population']:,} and "
            f"{c['school_count']} public schools, the county ranks in the "
            f"{'top' if c['composite_score'] >= 55 else 'bottom'} half nationally.\n\n"
            f"**Key Findings:**\n"
            f"- Education score: {c['education_score']}/100\n"
            f"- Health & resource access: {c['health_score']}/100\n"
            f"- Environmental quality: {c['environment_score']}/100\n"
            f"- Safety index: {c['safety_score']}/100\n\n"
            f"**Year-over-Year Change:** {c['score_change_1y']:+.1f} points\n\n"
            f"**Recommendation:** "
            f"{'Maintain current programs and share best practices with neighboring counties.' if cat in ('thriving', 'moderate') else 'Prioritize funding for health infrastructure and school resource programs. Coordinate with state agencies for environmental remediation.'}"
        )

        briefs.append(
            {
                "fips": c["fips"],
                "county_name": c["name"],
                "state": c["state"],
                "brief": brief_text,
                "generated_at": (now - timedelta(days=random.randint(0, 7))).isoformat(),
            }
        )

    _write(OUT / "county_ai_briefs.csv", briefs)

    # ── quality flags (~10% of schools) ─────────────────────────────────────
    flags: list[dict] = []
    flag_schools = random.sample(scores, k=int(len(scores) * 0.10))
    for sc in flag_schools:
        ft = random.choice(FLAG_TYPES)
        reasons = {
            "missing_data": "One or more pillar scores could not be computed due to missing source data",
            "outlier": "Composite score deviates >2 std from state mean",
            "stale_source": "CDC PLACES data for this county is >18 months old",
            "inconsistency": "Education score and enrollment data show conflicting trends",
        }
        flags.append(
            {
                "nces_id": sc["nces_id"],
                "school_name": sc["school_name"],
                "state": sc["state"],
                "flag_type": ft,
                "flag_reason": reasons[ft],
                "confidence": round(random.uniform(0.6, 0.99), 2),
                "flagged_at": (now - timedelta(days=random.randint(0, 30))).isoformat(),
            }
        )

    _write(OUT / "quality_flags.csv", flags)


if __name__ == "__main__":
    generate()
