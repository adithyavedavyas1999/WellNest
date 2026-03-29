"""
PDF county report generator using fpdf2.

Produces a branded, print-ready A4 report for a given county FIPS code.
Data comes from gold schema tables via SQLAlchemy.  Layout is intentionally
simple — no external font files, no embedded images — so this runs
anywhere without system dependencies (unlike the WeasyPrint path).

Color palette matches the dashboard:
  #C73E1D  critical  (0-25)
  #F18F01  at risk   (26-50)
  #2E86AB  moderate  (51-75)
  #3BB273  thriving  (76-100)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fpdf import FPDF
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger("wellnest.reports.pdf")

# WellNest brand palette
COLORS: dict[str, tuple[int, int, int]] = {
    "primary": (30, 58, 95),       # dark navy for headings
    "accent": (46, 134, 171),      # #2E86AB
    "critical": (199, 62, 29),     # #C73E1D
    "at_risk": (241, 143, 1),      # #F18F01
    "moderate": (46, 134, 171),    # #2E86AB
    "thriving": (59, 178, 115),    # #3BB273
    "white": (255, 255, 255),
    "light_gray": (240, 240, 240),
    "dark_text": (33, 37, 41),
    "muted_text": (108, 117, 125),
}


def _score_color(score: float | None) -> tuple[int, int, int]:
    """Map a 0-100 score to its category color."""
    if score is None:
        return COLORS["muted_text"]
    if score <= 25:
        return COLORS["critical"]
    if score <= 50:
        return COLORS["at_risk"]
    if score <= 75:
        return COLORS["moderate"]
    return COLORS["thriving"]


def _score_label(score: float | None) -> str:
    if score is None:
        return "Insufficient Data"
    if score <= 25:
        return "Critical"
    if score <= 50:
        return "At Risk"
    if score <= 75:
        return "Moderate"
    return "Thriving"


def _fmt(val: Any, suffix: str = "", decimals: int = 1) -> str:
    if val is None:
        return "N/A"
    try:
        return f"{float(val):.{decimals}f}{suffix}"
    except (ValueError, TypeError):
        return str(val)


class CountyReportGenerator:
    """Builds a multi-section PDF report for one county.

    Usage::

        gen = CountyReportGenerator(db_url="postgresql://...")
        gen.generate("17031")  # Cook County, IL
        gen.save(Path("reports/output/county_17031.pdf"))
    """

    def __init__(self, db_url: str) -> None:
        self._db_url: str = db_url
        self._engine = create_engine(db_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine)
        self._pdf: FPDF | None = None
        self._county: dict[str, Any] = {}
        self._schools: list[dict[str, Any]] = []
        self._gaps: list[dict[str, Any]] = []
        self._brief: str | None = None

    def generate(self, fips: str) -> CountyReportGenerator:
        """Pull data and build the full PDF for the given county FIPS."""
        self._load_data(fips)

        self._pdf = FPDF(orientation="P", unit="mm", format="A4")
        self._pdf.set_auto_page_break(auto=True, margin=20)
        self._pdf.add_page()

        self._add_header()
        self._add_overview()
        self._add_score_breakdown()
        self._add_schools_table()
        self._add_resource_gaps()
        self._add_ai_brief()
        self._add_footer()

        return self

    def save(self, path: Path) -> Path:
        """Write the built PDF to disk."""
        if self._pdf is None:
            raise RuntimeError("Call generate() before save()")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._pdf.output(str(path))
        logger.info("PDF saved to %s", path)
        return path

    # ------------------------------------------------------------------
    # data loading
    # ------------------------------------------------------------------

    def _load_data(self, fips: str) -> None:
        session: Session = self._session_factory()
        try:
            self._county = self._fetch_county(session, fips)
            self._schools = self._fetch_schools(session, fips)
            self._gaps = self._fetch_resource_gaps(session, fips)
            self._brief = self._fetch_ai_brief(session, fips)
        finally:
            session.close()

    def _fetch_county(self, db: Session, fips: str) -> dict[str, Any]:
        row = db.execute(
            text("""
                SELECT
                    c.county_fips AS fips,
                    c.county_name,
                    c.state_abbr AS state,
                    c.avg_wellbeing_score AS composite_score,
                    c.scored_school_count AS school_count,
                    c.total_population AS population,
                    c.avg_education_score AS education_score,
                    c.avg_health_score AS health_score,
                    c.avg_environment_score AS environment_score,
                    c.avg_safety_score AS safety_score,
                    c.avg_poverty_rate,
                    c.avg_chronic_absenteeism,
                    c.thriving_count,
                    c.moderate_count,
                    c.at_risk_count,
                    c.critical_count,
                    c.schools_with_gaps,
                    c.national_rank,
                    c.total_counties
                FROM gold.county_summary c
                WHERE c.county_fips = :fips
            """),
            {"fips": fips},
        ).mappings().first()

        if not row:
            raise ValueError(f"County {fips} not found in gold.county_summary")
        return dict(row)

    def _fetch_schools(self, db: Session, fips: str) -> list[dict[str, Any]]:
        rows = db.execute(
            text("""
                SELECT
                    s.nces_school_id AS nces_id,
                    s.school_name,
                    s.wellbeing_score AS composite_score,
                    s.education_score,
                    s.health_score,
                    s.environment_score,
                    s.safety_score,
                    s.total_enrollment AS enrollment,
                    s.wellbeing_category AS category
                FROM gold.child_wellbeing_score s
                WHERE s.county_fips = :fips
                ORDER BY s.wellbeing_score ASC
                LIMIT 50
            """),
            {"fips": fips},
        ).mappings().all()
        return [dict(r) for r in rows]

    def _fetch_resource_gaps(self, db: Session, fips: str) -> list[dict[str, Any]]:
        rows = db.execute(
            text("""
                SELECT
                    g.school_name,
                    g.weakest_pillar,
                    g.gap_count,
                    g.pillar_spread,
                    g.intervention_priority,
                    g.wellbeing_score
                FROM gold.resource_gaps g
                WHERE g.county_fips = :fips
                ORDER BY g.gap_count DESC, g.pillar_spread DESC
                LIMIT 20
            """),
            {"fips": fips},
        ).mappings().all()
        return [dict(r) for r in rows]

    def _fetch_ai_brief(self, db: Session, fips: str) -> str | None:
        row = db.execute(
            text("""
                SELECT brief_text
                FROM gold.county_ai_briefs
                WHERE county_fips = :fips
                ORDER BY generated_at DESC
                LIMIT 1
            """),
            {"fips": fips},
        ).mappings().first()
        return row["brief_text"] if row else None

    # ------------------------------------------------------------------
    # PDF sections
    # ------------------------------------------------------------------

    def _add_header(self) -> None:
        pdf = self._pdf
        assert pdf is not None

        # navy banner
        pdf.set_fill_color(*COLORS["primary"])
        pdf.rect(0, 0, 210, 38, "F")

        pdf.set_text_color(*COLORS["white"])
        pdf.set_font("Helvetica", "B", 22)
        pdf.set_y(8)
        pdf.cell(0, 10, "WellNest County Report", align="C", new_x="LMARGIN", new_y="NEXT")

        county_name = self._county.get("county_name", "Unknown")
        state = self._county.get("state", "")
        pdf.set_font("Helvetica", "", 14)
        pdf.cell(0, 8, f"{county_name}, {state}", align="C", new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(*COLORS["dark_text"])
        pdf.set_y(42)

    def _add_overview(self) -> None:
        pdf = self._pdf
        assert pdf is not None
        c = self._county

        composite = c.get("composite_score")
        color = _score_color(composite)
        label = _score_label(composite)

        # score badge
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "County Overview", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        pdf.set_font("Helvetica", "B", 28)
        pdf.set_text_color(*color)
        score_display = _fmt(composite, "/100", decimals=1)
        pdf.cell(60, 14, score_display, new_x="END")

        pdf.set_font("Helvetica", "", 12)
        pdf.set_text_color(*COLORS["muted_text"])
        pdf.cell(0, 14, f"  {label}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(*COLORS["dark_text"])
        pdf.ln(2)

        # quick stats row
        pdf.set_font("Helvetica", "", 10)
        stats = [
            ("Schools", str(c.get("school_count", "N/A"))),
            ("Population", f"{c['population']:,}" if c.get("population") else "N/A"),
            ("National Rank", f"#{c['national_rank']} of {c['total_counties']}"
             if c.get("national_rank") else "N/A"),
        ]
        for stat_label, stat_val in stats:
            pdf.cell(60, 7, f"{stat_label}: {stat_val}")
        pdf.ln(10)

    def _add_score_breakdown(self) -> None:
        pdf = self._pdf
        assert pdf is not None
        c = self._county

        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Pillar Score Breakdown", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        pillars = [
            ("Education", c.get("education_score"), "30%"),
            ("Health & Resources", c.get("health_score"), "30%"),
            ("Environment", c.get("environment_score"), "20%"),
            ("Safety", c.get("safety_score"), "20%"),
        ]

        # header row
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(*COLORS["primary"])
        pdf.set_text_color(*COLORS["white"])
        pdf.cell(65, 8, "Pillar", border=1, fill=True)
        pdf.cell(30, 8, "Score", border=1, fill=True, align="C")
        pdf.cell(30, 8, "Category", border=1, fill=True, align="C")
        pdf.cell(30, 8, "Weight", border=1, fill=True, align="C", new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(*COLORS["dark_text"])
        pdf.set_font("Helvetica", "", 10)

        for i, (name, score, weight) in enumerate(pillars):
            if i % 2 == 1:
                pdf.set_fill_color(*COLORS["light_gray"])
                fill = True
            else:
                fill = False

            pdf.cell(65, 7, name, border=1, fill=fill)

            color = _score_color(score)
            pdf.set_text_color(*color)
            pdf.cell(30, 7, _fmt(score), border=1, fill=fill, align="C")
            pdf.cell(30, 7, _score_label(score), border=1, fill=fill, align="C")
            pdf.set_text_color(*COLORS["dark_text"])
            pdf.cell(30, 7, weight, border=1, fill=fill, align="C", new_x="LMARGIN", new_y="NEXT")

        # category distribution
        pdf.ln(4)
        pdf.set_font("Helvetica", "", 9)
        dist_parts = [
            f"Thriving: {c.get('thriving_count', 0)}",
            f"Moderate: {c.get('moderate_count', 0)}",
            f"At Risk: {c.get('at_risk_count', 0)}",
            f"Critical: {c.get('critical_count', 0)}",
        ]
        pdf.cell(0, 6, "School distribution:  " + "  |  ".join(dist_parts),
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(6)

    def _add_schools_table(self) -> None:
        pdf = self._pdf
        assert pdf is not None

        if not self._schools:
            return

        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Schools in County (Lowest Scoring)", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        # header
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(*COLORS["primary"])
        pdf.set_text_color(*COLORS["white"])
        col_widths = [60, 22, 22, 22, 22, 22, 20]
        headers = ["School", "Score", "Edu", "Health", "Env", "Safety", "Enroll"]
        for w, h in zip(col_widths, headers):
            pdf.cell(w, 7, h, border=1, fill=True, align="C")
        pdf.ln()

        pdf.set_text_color(*COLORS["dark_text"])
        pdf.set_font("Helvetica", "", 7)

        display_count = min(len(self._schools), 15)
        for i in range(display_count):
            school = self._schools[i]
            if i % 2 == 1:
                pdf.set_fill_color(*COLORS["light_gray"])
                fill = True
            else:
                fill = False

            name = str(school.get("school_name", ""))[:32]
            pdf.cell(col_widths[0], 6, name, border=1, fill=fill)

            score_val = school.get("composite_score")
            color = _score_color(score_val)
            pdf.set_text_color(*color)
            pdf.cell(col_widths[1], 6, _fmt(score_val), border=1, fill=fill, align="C")
            pdf.set_text_color(*COLORS["dark_text"])

            for idx, key in enumerate(["education_score", "health_score",
                                        "environment_score", "safety_score"]):
                pdf.cell(col_widths[idx + 2], 6, _fmt(school.get(key)),
                         border=1, fill=fill, align="C")

            enroll = school.get("enrollment")
            enroll_str = f"{enroll:,}" if enroll else "N/A"
            pdf.cell(col_widths[6], 6, enroll_str, border=1, fill=fill,
                     align="C", new_x="LMARGIN", new_y="NEXT")

        if len(self._schools) > 15:
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(0, 6, f"... and {len(self._schools) - 15} more schools",
                     new_x="LMARGIN", new_y="NEXT")
        pdf.ln(6)

    def _add_resource_gaps(self) -> None:
        pdf = self._pdf
        assert pdf is not None

        if not self._gaps:
            return

        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "Resource Gap Analysis", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 6,
                 f"{self._county.get('schools_with_gaps', 0)} schools with identified resource gaps",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        # table header
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(*COLORS["primary"])
        pdf.set_text_color(*COLORS["white"])
        gap_widths = [55, 30, 20, 25, 30]
        gap_headers = ["School", "Weakest Pillar", "Gaps", "Spread", "Priority"]
        for w, h in zip(gap_widths, gap_headers):
            pdf.cell(w, 7, h, border=1, fill=True, align="C")
        pdf.ln()

        pdf.set_text_color(*COLORS["dark_text"])
        pdf.set_font("Helvetica", "", 7)

        display_count = min(len(self._gaps), 10)
        for i in range(display_count):
            gap = self._gaps[i]
            fill = i % 2 == 1
            if fill:
                pdf.set_fill_color(*COLORS["light_gray"])

            name = str(gap.get("school_name", ""))[:30]
            pdf.cell(gap_widths[0], 6, name, border=1, fill=fill)
            pdf.cell(gap_widths[1], 6, str(gap.get("weakest_pillar", "")),
                     border=1, fill=fill, align="C")
            pdf.cell(gap_widths[2], 6, str(gap.get("gap_count", "")),
                     border=1, fill=fill, align="C")
            pdf.cell(gap_widths[3], 6, _fmt(gap.get("pillar_spread")),
                     border=1, fill=fill, align="C")

            priority = str(gap.get("intervention_priority", ""))
            if priority == "High Priority":
                pdf.set_text_color(*COLORS["at_risk"])
            elif priority == "Critical Need":
                pdf.set_text_color(*COLORS["critical"])
            else:
                pdf.set_text_color(*COLORS["dark_text"])
            pdf.cell(gap_widths[4], 6, priority, border=1, fill=fill,
                     align="C", new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(*COLORS["dark_text"])

        pdf.ln(6)

    def _add_ai_brief(self) -> None:
        pdf = self._pdf
        assert pdf is not None

        if not self._brief:
            return

        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, "AI-Generated Community Brief", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        # light background box for the brief
        pdf.set_fill_color(248, 249, 250)
        y_before = pdf.get_y()
        pdf.set_font("Helvetica", "", 9)

        brief_clean = self._brief.replace("**", "").replace("##", "").strip()
        pdf.multi_cell(0, 5, brief_clean, fill=True)
        pdf.ln(6)

    def _add_footer(self) -> None:
        pdf = self._pdf
        assert pdf is not None

        pdf.ln(4)
        pdf.set_draw_color(*COLORS["muted_text"])
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(3)

        pdf.set_font("Helvetica", "I", 7)
        pdf.set_text_color(*COLORS["muted_text"])
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        pdf.cell(0, 5, f"Generated by WellNest on {timestamp}", new_x="LMARGIN", new_y="NEXT",
                 align="L")
        pdf.cell(0, 5,
                 "Data sources: NCES CCD/EDGE, CDC PLACES, Census ACS, EPA AQS, "
                 "HRSA HPSA/MUA, USDA Food Access, FEMA NRI, FBI UCR",
                 new_x="LMARGIN", new_y="NEXT", align="L")
        pdf.set_text_color(*COLORS["dark_text"])

    # ------------------------------------------------------------------
    # cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
