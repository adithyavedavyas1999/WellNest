"""
WellNest reports module — PDF generation and email delivery.

Typical usage::

    from reports import CountyReportGenerator, ReportEmailer

    gen = CountyReportGenerator(db_url="postgresql://...")
    gen.generate("17031").save(Path("output/county_17031.pdf"))

    emailer = ReportEmailer()
    emailer.send_report("admin@school.edu", "17031", Path("output/county_17031.pdf"))
"""

from reports.email_sender import ReportEmailer
from reports.pdf_generator import CountyReportGenerator

__all__: list[str] = [
    "CountyReportGenerator",
    "ReportEmailer",
]
