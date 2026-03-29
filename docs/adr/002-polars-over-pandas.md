# ADR-002: Polars over pandas for Data Processing

**Status:** Accepted
**Date:** 2024-08
**Author:** Engineering

## Context

WellNest's ingestion layer processes 12 federal data sources. The largest single dataset is the Census ACS at ~74,000 census tracts with ~25 columns. The NCES schools dataset has ~130,000 rows. The feature engineering pipeline joins and transforms these into a ~130,000 x 40 matrix.

None of these are "big data" by any modern definition. Everything fits comfortably in memory on a laptop. The question isn't whether to use Spark -- it's whether to use pandas or something better for medium-sized datasets.

## Decision

We chose Polars as the primary dataframe library. pandas is only used when a third-party library (like GeoPandas) requires it, and we convert at the boundary.

## What We Considered

### pandas

The obvious default. Everyone knows it, every tutorial uses it, and the ecosystem is massive. I've used pandas on every data project for the past 6 years.

But pandas has real problems that I've run into repeatedly:

1. **Copy semantics are confusing.** The `SettingWithCopyWarning` is a rite of passage, but even experienced pandas users get tripped up by views vs copies. The recent copy-on-write changes in pandas 2.x help, but the mental model is still muddy.

2. **Type system is loose.** A column that should be `int64` silently becomes `float64` when you introduce a NaN. A datetime column becomes `object` if one row has a bad format. You discover these issues downstream, not when they happen.

3. **Memory usage is poor for strings.** pandas stores strings as Python objects by default. Our school names and county names eat 3-4x more memory than they should. The `string[pyarrow]` dtype helps, but you have to opt into it everywhere.

4. **API encourages mutation.** `df["new_col"] = ...` modifies in place. `df.drop(..., inplace=True)` is a footgun. The functional-style API (`df.assign(...)`, `df.pipe(...)`) exists but isn't the default pattern people reach for.

**What we liked:** Universal familiarity, massive ecosystem, every library interoperates with it.

**Why we rejected it:** Performance is 3-5x worse than Polars on our workloads (benchmarked below). Memory usage is notably worse with string-heavy data. The API encourages patterns that lead to subtle bugs.

### Polars

Polars is a DataFrame library written in Rust with a Python API. It's been gaining traction since 2022 and hit 1.0 in 2024.

### DuckDB

DuckDB is an in-process analytical database. We use it alongside Polars for some SQL-style operations, but it's more of a complement than a replacement for a DataFrame library. DuckDB shines for analytical queries but doesn't have the same DataFrame manipulation API surface.

## Why Polars Won

### 1. Performance on our actual workloads

I benchmarked the Census ACS ingestion pipeline (the largest single-source transform) on my M2 MacBook Air:

| Operation | pandas 2.x | Polars 1.x | Speedup |
|-----------|-----------|-----------|---------|
| CSV read (74K rows, 25 cols) | 1.2s | 0.3s | 4.0x |
| Type casting (all numeric cols) | 0.4s | 0.08s | 5.0x |
| Group-by aggregation (by county) | 0.3s | 0.06s | 5.0x |
| Join (schools x census, 130K x 74K) | 2.1s | 0.5s | 4.2x |
| Full ingestion pipeline | 8.3s | 2.1s | 3.9x |

These aren't cherry-picked -- they're the actual operations in our `census_acs.py` connector. The speedup comes from Polars' multi-threaded execution and Apache Arrow memory format.

For our total pipeline (all 12 sources through feature engineering), the wall-clock difference is about 45 seconds (pandas) vs 12 seconds (Polars). Not life-changing, but when you're iterating on feature engineering or debugging a transform, the faster feedback loop adds up.

### 2. Memory efficiency

The NCES schools dataset (130K rows, 30+ columns including several string columns) uses:
- pandas: ~180MB in memory
- Polars: ~55MB in memory

The difference is almost entirely due to string handling. Polars uses Apache Arrow's string representation (offsets into a contiguous buffer), while pandas uses Python objects (each string is a separate heap allocation). With 130K school names, city names, and county names, this adds up fast.

For our full feature matrix (~130K rows x 40 columns), the difference is ~320MB (pandas) vs ~95MB (Polars). Neither is a problem on a modern laptop, but the Polars approach is clearly more efficient.

### 3. API expressiveness

Polars' expression API is more composable than pandas. Consider null replacement for the Census data:

```python
# pandas
for col in numeric_cols:
    df[col] = df[col].replace(-666666666, np.nan)
    df[col] = pd.to_numeric(df[col], errors='coerce')

# polars
df = df.with_columns(
    pl.when(pl.col(col) == CENSUS_MISSING_VALUE)
    .then(None)
    .otherwise(pl.col(col))
    .alias(col)
    for col in numeric_cols
)
```

The Polars version is a single expression that generates an optimized execution plan. The pandas version is a loop that modifies the DataFrame in place, column by column.

Polars' lazy evaluation API is even better for complex transforms -- you build a query plan and Polars optimizes it before executing. We don't use lazy mode everywhere (eager is fine for our data sizes), but it catches type errors at plan-build time rather than at execution time, which is a nice safety net.

### 4. Immutability by default

Polars DataFrames are immutable. Every operation returns a new DataFrame. There's no `inplace=True`, no `SettingWithCopyWarning`, no ambiguity about whether you're modifying a view or a copy.

This matters more than you'd think. In our ingestion pipelines, data flows through extract -> transform -> validate -> load. With immutable DataFrames, each step is a pure function that takes a DataFrame and returns a new one. The old DataFrame is still there if you need to debug. With pandas, a rogue `inplace=True` somewhere can silently corrupt data flowing downstream.

### 5. Better null handling

Polars distinguishes between "null" (missing) and "NaN" (not a number), unlike pandas which conflates them for float columns. This matters for our scoring pipeline where we need to distinguish "the data source doesn't have this metric for this school" (null) from "the computation produced an undefined result" (NaN). In pandas, both become `NaN`, and you can't tell them apart without auxiliary tracking.

## Ecosystem Trade-offs

The biggest downside of Polars is ecosystem support. Libraries that expect pandas DataFrames:

- **GeoPandas:** We use GeoPandas for spatial joins (school-to-tract matching). We convert Polars -> pandas at the boundary, do the spatial operation, and convert back. This is about 10 lines of glue code in `ingestion/utils/geo_utils.py`. Slightly annoying but manageable.

- **Streamlit:** Streamlit's `st.dataframe()` and `st.table()` accept pandas or native Python types. We convert to pandas in the dashboard layer. This is a few `.to_pandas()` calls in `dashboard/utils/db.py`.

- **scikit-learn / XGBoost:** These expect numpy arrays, not DataFrames. Both pandas and Polars convert to numpy via `.to_numpy()`, so there's no difference here.

- **SQLAlchemy / write_database:** Polars has built-in `write_database()` that uses SQLAlchemy under the hood. Works fine for our use case.

The ecosystem gap is real but narrowing fast. Most libraries that accept pandas also accept anything with an `__array__` or `__dataframe__` protocol. And for our project specifically, the boundary conversions are confined to a few utility functions.

## Consequences

### Positive

- 3-5x faster pipeline execution. Not critical at our data sizes, but the faster feedback loop during development is genuinely useful.
- ~3x better memory efficiency, mainly from string handling.
- Immutable DataFrames eliminate an entire class of mutation-related bugs.
- Better null semantics for our scoring pipeline.
- Expression API encourages composable, readable transforms.

### Negative

- New team members may not know Polars. There's a learning curve, though the API is well-documented and conceptually similar to SQL.
- Some libraries require pandas conversion at the boundary. This is a few lines of glue code but adds friction.
- Polars is younger than pandas. Edge cases in obscure operations may be less polished. We haven't hit this yet, but it's a risk.
- Stack Overflow and tutorial coverage is much smaller. When you hit a wall, you're more likely to need to read the Polars docs or source code rather than finding a ready-made answer.

### Migration Strategy

We don't need to convert everything overnight. The rule is:
- New code uses Polars
- Existing code stays as-is unless we're already modifying it
- GeoPandas operations stay in pandas (no Polars equivalent yet)
- Dashboard layer converts at the boundary

The `pyproject.toml` has a comment: "switched from pandas -- never looking back." That's honestly how it feels. The ergonomics are better and the performance is free.
