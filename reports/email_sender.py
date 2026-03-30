"""
Email delivery for WellNest PDF reports.

Sends county reports as PDF attachments over SMTP with TLS.  Config comes
from environment variables so it works the same in local dev (Mailpit/Mailtrap)
and production (SES, SendGrid, etc.).

Env vars:
  SMTP_HOST      default: localhost
  SMTP_PORT      default: 587
  SMTP_USER      (optional — skip auth if blank)
  SMTP_PASSWORD  (optional)
  SMTP_FROM      default: reports@wellnest.chieac.org
  SMTP_USE_TLS   default: true
"""

from __future__ import annotations

import logging
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

logger = logging.getLogger("wellnest.reports.email")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


class ReportEmailer:
    """Send county PDF reports via email with an HTML body.

    Usage::

        emailer = ReportEmailer()
        emailer.send_report(
            recipient="admin@school.edu",
            fips="17031",
            pdf_path=Path("reports/output/county_17031.pdf"),
        )

    For bulk sends::

        emailer.send_batch([
            {"recipient": "a@b.org", "fips": "17031", "pdf_path": Path(...)},
            {"recipient": "c@d.org", "fips": "06037", "pdf_path": Path(...)},
        ])
    """

    def __init__(
        self,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        smtp_user: str | None = None,
        smtp_password: str | None = None,
        from_address: str | None = None,
        use_tls: bool | None = None,
    ) -> None:
        self.host: str = smtp_host or _env("SMTP_HOST", "localhost")
        self.port: int = smtp_port or int(_env("SMTP_PORT", "587"))
        self.user: str = smtp_user or _env("SMTP_USER", "")
        self.password: str = smtp_password or _env("SMTP_PASSWORD", "")
        self.from_address: str = from_address or _env("SMTP_FROM", "reports@wellnest.chieac.org")

        if use_tls is not None:
            self.use_tls = use_tls
        else:
            self.use_tls = _env("SMTP_USE_TLS", "true").lower() in ("true", "1", "yes")

    def send_report(
        self,
        recipient: str,
        fips: str,
        pdf_path: Path,
        county_data: dict[str, Any] | None = None,
    ) -> bool:
        """Send a single county report email with the PDF attached.

        Returns True on success, False on failure (logged, not raised).
        """
        if not pdf_path.exists():
            logger.error("PDF not found: %s", pdf_path)
            return False

        county_name = (county_data or {}).get("county_name", f"County {fips}")
        state = (county_data or {}).get("state", "")
        subject = (
            f"WellNest County Report — {county_name}, {state}"
            if state
            else (f"WellNest County Report — {county_name}")
        )

        msg = MIMEMultipart("mixed")
        msg["From"] = self.from_address
        msg["To"] = recipient
        msg["Subject"] = subject

        html_body = self._build_html_body(county_data or {"county_name": county_name, "fips": fips})
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        pdf_bytes = pdf_path.read_bytes()
        attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
        filename = f"wellnest_county_{fips}.pdf"
        attachment.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(attachment)

        try:
            self._send_smtp(msg)
            logger.info("Report email sent to %s (FIPS %s)", recipient, fips)
            return True
        except Exception:
            logger.exception("Failed to send email to %s", recipient)
            return False

    def send_batch(self, recipients: list[dict[str, Any]]) -> dict[str, int]:
        """Send reports to multiple recipients.

        Each entry in `recipients` should have:
          - recipient: email address
          - fips: county FIPS code
          - pdf_path: Path to the PDF file
          - county_data: (optional) dict with county_name, state, etc.

        Returns counts: {"sent": N, "failed": M}
        """
        sent = 0
        failed = 0

        for entry in recipients:
            ok = self.send_report(
                recipient=entry["recipient"],
                fips=entry["fips"],
                pdf_path=Path(entry["pdf_path"]),
                county_data=entry.get("county_data"),
            )
            if ok:
                sent += 1
            else:
                failed += 1

        logger.info("Batch complete: %d sent, %d failed", sent, failed)
        return {"sent": sent, "failed": failed}

    def _build_html_body(self, county_data: dict[str, Any]) -> str:
        """Build the HTML email body with inline styles (no CSS classes).

        Email clients strip <style> blocks, so everything has to be inline.
        Keeping it simple — just a header, brief message, and CTA.
        """
        county_name = county_data.get("county_name", "your county")
        fips = county_data.get("fips", "")
        score = county_data.get("composite_score")
        state = county_data.get("state", "")

        score_html = ""
        if score is not None:
            if score <= 25:
                score_color = "#C73E1D"
            elif score <= 50:
                score_color = "#F18F01"
            elif score <= 75:
                score_color = "#2E86AB"
            else:
                score_color = "#3BB273"
            score_html = f"""
            <p style="font-size: 28px; font-weight: 700; color: {score_color};
                       margin: 12px 0 4px 0;">
                {score:.1f}/100
            </p>
            """

        location = f"{county_name}, {state}" if state else county_name

        return f"""\
<div style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            max-width: 600px; margin: 0 auto; color: #212529;">

    <div style="background: #1E3A5F; color: white; padding: 20px 24px;
                border-radius: 4px 4px 0 0;">
        <h1 style="margin: 0; font-size: 22px; font-weight: 700;">
            WellNest County Report
        </h1>
        <p style="margin: 4px 0 0 0; opacity: 0.9; font-size: 14px;">
            {location}
        </p>
    </div>

    <div style="padding: 24px; background: #ffffff; border: 1px solid #dee2e6;
                border-top: none; border-radius: 0 0 4px 4px;">

        <p style="font-size: 14px; line-height: 1.6;">
            Hi,
        </p>
        <p style="font-size: 14px; line-height: 1.6;">
            Your WellNest County Report for <strong>{location}</strong>
            (FIPS: {fips}) is attached as a PDF.
        </p>

        {score_html}

        <p style="font-size: 14px; line-height: 1.6;">
            The report includes pillar score breakdowns, school-level data,
            resource gap analysis, and an AI-generated community brief.
        </p>

        <p style="font-size: 14px; line-height: 1.6; margin-top: 20px;">
            View the full interactive dashboard at
            <a href="https://wellnest.chieac.org" style="color: #2E86AB;">
                wellnest.chieac.org
            </a>
        </p>

        <hr style="border: none; border-top: 1px solid #dee2e6; margin: 20px 0;" />

        <p style="font-size: 11px; color: #6c757d; line-height: 1.5;">
            This report was generated by WellNest, a project of the Chicago
            Education &amp; Analytics Collaborative (ChiEAC). Data sources include
            NCES, CDC, Census Bureau, EPA, HRSA, USDA, FEMA, and FBI.
        </p>
    </div>
</div>
"""

    def _send_smtp(self, msg: MIMEMultipart) -> None:
        """Low-level SMTP send with TLS support."""
        if self.use_tls:
            server = smtplib.SMTP(self.host, self.port, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP(self.host, self.port, timeout=30)
            server.ehlo()

        try:
            if self.user and self.password:
                server.login(self.user, self.password)
            server.send_message(msg)
        finally:
            server.quit()
