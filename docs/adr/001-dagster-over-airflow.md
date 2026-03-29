# ADR-001: Dagster over Airflow for Orchestration

**Status:** Accepted
**Date:** 2024-08
**Author:** Engineering

## Context

We need an orchestrator to manage WellNest's data pipelines: ingesting 12 federal data sources, running dbt transformations, training ML models, generating AI briefs, and running quality checks. The pipeline runs weekly, with a daily sub-pipeline for weather alerts and a monthly sub-pipeline for AI content.

The team has Airflow experience from past projects. Dagster and Prefect are the main alternatives we evaluated.

## Decision

We chose Dagster.

## What We Considered

### Apache Airflow

Airflow is the incumbent. I've used it on three previous projects and know the ecosystem well. It's battle-tested, has a massive community, and every cloud provider offers a managed version.

But Airflow's DAG-centric model is a mismatch for what we're building. In Airflow, you define a DAG of tasks, and each task is an opaque function that "does something." There's no first-class concept of "this task produces a dataset" -- you have to bolt that on with XCom or external metadata stores. The disconnect between "the pipeline ran successfully" and "the data is correct" is a constant source of confusion.

Airflow's local development experience is also rough. You need a metadata database, a scheduler, and a webserver just to test a single DAG. The `airflow standalone` command helps but it's still heavier than it should be. And the DAG parsing delay (Airflow re-scans Python files every 30 seconds) makes iteration slow.

**What we liked:** Massive ecosystem, managed offerings (MWAA, Cloud Composer, Astronomer), proven at scale.

**Why we rejected it:** The task-centric model doesn't map well to our dbt-centric workflow. We'd need custom operators to represent dbt models as first-class assets. The local dev experience is too heavy for a small team. And frankly, the configuration complexity (executor types, worker pools, connection management) is more infrastructure than we want to maintain.

### Prefect

Prefect 2.x was a serious contender. The Python-native API is clean, the local development story is good, and the free tier of Prefect Cloud handles scheduling and monitoring.

The main concern was maturity for our use case. Prefect's asset/artifact tracking is more limited than Dagster's. In Dagster, a dbt model is a software-defined asset with upstream dependencies, metadata, and I/O managers. In Prefect, it's a task that happens to run dbt. The distinction matters when you have 20+ dbt models with complex dependencies.

**What we liked:** Clean Python API, good local dev experience, Prefect Cloud free tier.

**Why we rejected it:** Less native integration with dbt. The asset/lineage story is weaker than Dagster's. The community is smaller (harder to find answers to obscure issues). Also, Prefect's pivot from 1.x to 2.x to 3.x shook our confidence in API stability -- we don't want to rewrite orchestration code every 18 months.

### Dagster

Dagster's core concept is the **software-defined asset**: a function that produces a named piece of data, with declared upstream dependencies. This maps perfectly to our architecture:

- Each raw ingestion source is an asset
- Each dbt staging model is an asset
- Each dbt silver/gold model is an asset
- Each ML model training run is an asset
- Each AI brief generation is an asset

When you materialize an asset, Dagster knows what upstream assets it depends on and can materialize them first. The lineage graph is built automatically from the code. There's no separate DAG definition to keep in sync with the actual data flow.

## Why Dagster Won

### 1. Software-defined assets fit our architecture

Our pipeline is fundamentally about producing datasets (raw tables, staging models, gold scores, predictions). Dagster's asset model represents this directly. In our `orchestration/assets/` directory, each file defines a set of assets with their dependencies:

```python
@asset(group_name="bronze", deps=["config"])
def bronze_nces_schools(http_client: HttpClientResource, postgres: PostgresResource):
    connector = NCESConnector(...)
    return connector.run()
```

The dependency graph is the code. There's no separate DAG YAML to maintain.

### 2. dbt integration is first-class

Dagster has a `dagster-dbt` package that turns every dbt model into a Dagster asset. We wrapped it slightly (in `orchestration/resources.py`) to inject our project paths, but the core integration works out of the box. When our bronze assets finish materializing, Dagster automatically knows to run the staging models, then silver, then gold.

### 3. Type system catches bugs early

Dagster's resources are Pydantic-based `ConfigurableResource` classes. If we misconfigure a database connection or forget an API key, we get a clear error at startup rather than a runtime failure 20 minutes into a pipeline run. This has already saved us time multiple times during development.

### 4. Local development is painless

`dagster dev` starts a webserver and daemon in a single process. You can materialize individual assets, inspect their metadata, and view the lineage graph -- all without Docker, without a separate scheduler process, without configuring an executor. The feedback loop is fast.

### 5. Testing is straightforward

Assets are just Python functions. You can unit test them by calling them directly with mock resources. This is dramatically easier than testing Airflow operators, which require mocking the Airflow context, task instance, XCom, etc.

## Consequences

### Positive

- The pipeline code is clean and declarative. New team members can understand the data flow by looking at the asset graph in the Dagster UI.
- dbt integration works seamlessly. Each dbt model shows up as a node in the asset graph with correct upstream/downstream relationships.
- The type system catches configuration errors at startup.
- Testing is simple -- assets are just functions.
- `dagster dev` makes local iteration fast.

### Negative

- Smaller community than Airflow. When we hit obscure issues, there are fewer Stack Overflow answers and blog posts to reference.
- No major managed offering. Dagster Cloud exists but is expensive for our use case ($0/month target). We run Dagster in Docker for now.
- The team has to learn a new tool. The asset model is conceptually different from Airflow's DAG model, and it takes a few days to internalize.
- Dagster's documentation can be uneven. The happy path is well-documented, but edge cases (custom I/O managers, multi-asset groups, dynamic partitions) sometimes require reading the source code.
- Vendor lock-in risk: if Dagster Inc. pivots or goes under, we'd need to migrate. But the actual pipeline logic is in plain Python and dbt -- only the orchestration glue would need rewriting.

### Things to Watch

- Dagster's release cadence is aggressive (~monthly minor releases). We pin to a specific minor version in `pyproject.toml` and upgrade deliberately.
- Memory usage: the Dagster webserver can use 500MB-1GB with large asset graphs. Not an issue for our ~50 assets, but something to monitor if we grow.
- The `dagster-postgres` storage backend is the right choice for durability, but it means we need a separate `dagster` database alongside the `wellnest` application database. The `init-db.sql` handles this.
