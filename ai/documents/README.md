# Policy Documents for RAG Index

Drop PDF files into this directory and they'll be indexed by the RAG pipeline
(`ai/rag/indexer.py`).  The indexer runs as a Dagster asset on a monthly
schedule, or you can trigger it manually.

## Recommended Documents

These are the core policy documents we use for the community Q&A chatbot.
Download them and place the PDFs here.

### Federal Education Policy

| Document | Source | Notes |
|----------|--------|-------|
| ESSA State Plan Guidance | [ed.gov](https://www.ed.gov/essa) | Every Student Succeeds Act implementation guidance.  Get the consolidated state plan template — it covers accountability, assessments, and school improvement requirements. |
| Title I, Part A Guidance | [ed.gov](https://www2.ed.gov/programs/titleiparta/index.html) | Funding formulas, eligibility, and program requirements for Title I schools.  The non-regulatory guidance PDF is the most useful one. |
| Title IV, Part A Guidance | [ed.gov](https://oese.ed.gov/offices/office-of-formula-grants/safe-supportive-schools/title-iv-part-a-student-support-and-academic-enrichment-program/) | Student Support and Academic Enrichment Grants — covers well-rounded education, safe schools, and technology. |
| IDEA Part B Regulations | [ed.gov](https://sites.ed.gov/idea/) | Special education requirements.  Relevant for the education pillar scoring. |

### Health & Wellbeing

| Document | Source | Notes |
|----------|--------|-------|
| CDC School Health Guidelines | [cdc.gov](https://www.cdc.gov/healthyschools/) | Comprehensive framework for school health programs.  Look for "Health Education Curriculum Analysis Tool" (HECAT) and the Whole School, Whole Community, Whole Child (WSCC) model. |
| CDC PLACES Methodology | [cdc.gov](https://www.cdc.gov/places/) | Technical documentation for the health measures we use in scoring.  Helpful for answering questions about how health indicators are calculated. |
| HRSA Health Professional Shortage Areas | [hrsa.gov](https://bhw.hrsa.gov/shortage-designation) | Criteria for HPSA designation.  We use these in the health pillar. |
| Healthy People 2030 Objectives | [health.gov](https://health.gov/healthypeople) | National health objectives — useful reference for benchmarking county health scores. |

### Environment & Safety

| Document | Source | Notes |
|----------|--------|-------|
| EPA Air Quality Standards | [epa.gov](https://www.epa.gov/criteria-air-pollutants/naaqs-table) | NAAQS reference tables.  Relevant for environment pillar scoring. |
| FEMA National Risk Index Methodology | [fema.gov](https://hazards.fema.gov/nri/) | Technical docs for the NRI data we use in environment scoring. |
| FBI UCR Methodology | [fbi.gov](https://ucr.fbi.gov/) | How crime data is collected and reported.  Important caveats about UCR vs NIBRS transition. |

### Economic

| Document | Source | Notes |
|----------|--------|-------|
| USDA Food Access Research Atlas | [usda.gov](https://www.ers.usda.gov/data-products/food-access-research-atlas/) | Methodology for food desert classification.  We use this in the economic pillar. |
| Census ACS Subject Definitions | [census.gov](https://www.census.gov/programs-surveys/acs/technical-documentation/code-lists.html) | Variable definitions for poverty, income, insurance coverage, etc. |

## File Naming

No strict naming convention required — the indexer picks up all `*.pdf` files.
That said, it helps to use descriptive names:

```
essa_state_plan_guidance_2024.pdf
title_i_part_a_nonreg_guidance.pdf
cdc_wscc_model_framework.pdf
```

## What NOT to Put Here

- Don't add student-level data or PII documents
- Don't add massive datasets as PDFs (use the ingestion pipeline instead)
- Don't add copyrighted textbooks (the index is internal-only but still)
- Documents over 500 pages will work but will slow down the indexer significantly

## Rebuilding the Index

After adding new documents, rebuild the index:

```bash
# via Dagster (recommended)
dagster asset materialize --select ai_rag_index

# or directly
python -c "from ai.rag import DocumentIndexer; DocumentIndexer().index_all()"
```

The index files are written to `ai/rag/index/` and are gitignored.
