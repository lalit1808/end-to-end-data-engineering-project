# End-to-End Data Engineering Project

Built this to get real, hands-on DE experience - not just read about it.
Covers the whole thing: ingestion, orchestration, transformation, testing,
monitoring, alerting, CI. Used live crypto prices as the data, but this
isn't really a "crypto project" - it's a DE project that happens to use
crypto as the dataset.

## Flow

```
CoinGecko API
  -> Python (rate-limit handling, writes to Postgres)
  -> raw.daily_crypto (hourly snapshots, never deleted)
  -> raw.all_crypto_data (180-day backfill + auto-backfill for new coins)
  -> dbt staging (clean columns)
  -> dbt fact tables
       fact_daily_ohlc - open/close/high/low per finished day
       fact_price_history - full history, incremental, deduplicated
  -> dbt marts
       mart_daily_top_movers
       mart_hourly_top_movers
       dim_tracked_coins - today's actual top 25, by rank

Airflow: extract -> gap check -> dbt run -> dbt test -> dbt source
freshness, every hour. Slack alert on any task failure, on a run that
inserts zero rows, and on a detected gap in the hourly series.
GitHub Actions: real Postgres service, seeded sample data, dbt run + dbt
test on every push - not just a syntax check.
```

## Dashboard

Streamlit. Price chart + 7-day forecast (straight line trend, nothing
fancy, didn't want to fake sophistication). Daily movers with a date
picker. Hourly movers, live. Dropdown only shows current top-25 coins.

## Stack

Python, Postgres, dbt, Airflow, Docker, Streamlit, GitHub Actions, Slack.

## Design decisions

- **Idempotent writes.** Every insert uses `ON CONFLICT DO UPDATE` or
  `DO NOTHING` on a proper unique key (`coin_id, snapshot_at`, etc.). The
  pipeline can be re-run or retried without creating duplicates - this is
  what makes Airflow's automatic retries safe.
- **Rate limits.** CoinGecko's free tier is strict. Backfill retries with
  exponential backoff and pauses every few coins, or it dies halfway
  through a run.
- **Hourly snapshots, never overwritten.** Lets dbt compute real
  open/high/low/close from the actual intraday spread, not just one
  averaged number.
- **Continuous coin tracking.** Originally only fetched today's top 25,
  so a coin that dropped out just stopped getting data - permanent gap
  in its history. Fixed by tracking every coin ever seen, forever, using
  the raw `market_cap_rank` field (not just presence) to still filter
  `dim_tracked_coins` down to today's actual top 25 for display. The
  naive fix (just check "is this coin in the latest snapshot") would
  have broken once every coin started getting fetched every hour.
- **dbt never writes to raw.** All transformation is read-only against
  raw tables, so logic can be reworked freely with zero risk to ingested
  data.
- **Incremental model.** `fact_price_history` uses
  `materialized='incremental'` with `unique_key=['coin_id','price_date']`
  and `is_incremental()` to only reprocess the last 2 days each run,
  instead of rebuilding the full table every hour.
- **Day derived from the price, not the clock.** The hourly loader
  stamps rows with the insert date, so the 23:59 snapshot landed a few
  seconds after midnight and got filed under the next day - which made
  every day's "open" the previous day's closing price. Fixed in staging
  rather than in ingestion: `snapshot_at::date` becomes the reporting
  day and the original insert date is kept as `ingested_date`. Doing it
  in the transformation layer corrected all existing history in one
  rebuild and left raw untouched.
- **Grain is tested, not just assumed.** Both fact tables carry a
  `dbt_utils.unique_combination_of_columns` test on
  `(coin_id, price_date)`. The constraint already held by construction,
  but nothing asserted it, so a model change could have broken it
  silently with CI still green.
- **Gap detection on the hourly series.** Freshness only checks the
  front edge - "is the newest row recent?" - which stays green even when
  there are holes behind it. A separate check compares the last two
  snapshot hours and alerts with the number of hours missing.
- **Report what landed, not what was sent.** The loader used to log the
  row count it passed to the insert, which with `ON CONFLICT DO NOTHING`
  says nothing about what was written. It now reads `cur.rowcount` and
  alerts when a run inserts zero.
- **Secrets in `.env`.** Slack webhook, DB creds, Airflow login - all
  environment variables, git-ignored, never hardcoded.

## Running it

```bash
docker compose up -d
```

Airflow: localhost:8080. Postgres: 5432.

Unpause `crypto_hourly_dag`, it runs itself from there. `crypto_historical_dag`
is manual only - no reason to hit that expensive API call daily.

Needs a `.env` at the project root (not committed) - see `.env.example`
for the required variables.

dbt directly:
```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt source freshness --profiles-dir .
```

Dashboard:
```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

## Alerting and monitoring

Three signals, because they catch different failures:

- **Task failure** - any task in the hourly DAG posts to Slack.
- **Zero-insert alert** - a run that fetches rows but writes none.
  Without this, a successful task and an empty load look identical.
- **Gap detection** - compares the last two snapshot hours and reports
  how many hours are missing.
- **Source freshness** - warns at 2 hours stale, errors at 4.

What none of them catch: the host being down, since every check runs on
the same machine as the pipeline. That needs an external heartbeat, and
it's the next thing I'd add.

## Testing

23 tests across sources, staging, facts and marts - `not_null` and
`unique` on keys and measures, plus grain uniqueness on both fact
tables. Run locally, every hour in Airflow, and in CI against a real
temporary Postgres service on every push - actual tests against actual
data, not just a compile check.

## Bugs I found and fixed

Auditing my own pipeline turned up four real problems. Keeping them
here because how they were found matters as much as the code.

- **Seed rows silently dropped.** `fact_price_history` excluded seed
  rows using `price_date not in (select price_date from fact_daily_ohlc)`
  - a date-level filter, not per coin. So a newly backfilled coin lost
  its history wherever any *other* coin already had that date. Fixed
  with a correlated `not exists` on coin and date; recovered 130 rows.
- **No dedup across the union.** The same model stacks three sources
  with `union all` and had no final deduplication. Added a
  `row_number()` with source precedence, plus the grain tests that
  would have caught it.
- **Open and close shifted by an hour.** Covered above - the insert
  date was being used as the reporting day.
- **Empty loads reported as success.** Covered above - row count sent
  versus row count written.

One limitation I can't fix: the hourly snapshots come from a
current-price endpoint, so a missed window is gone permanently.
Airflow catch-up just refetches *now*, and the primary key correctly
rejects it as a duplicate. The daily history uses a different endpoint
that does serve the past, which is why that layer is fully
backfillable. Idempotent and recoverable aren't the same thing, and
which one you get is a property of the source.

## CI

GitHub Actions spins up Postgres, loads seed data, runs `dbt run` +
`dbt test` on every push.

## TODO

- External heartbeat monitoring, so a host outage is visible
- Better forecast at some point
- CD - needs the infra hosted somewhere reachable instead of my laptop,
  which is a scope call for a portfolio project, not something I missed

## Why

Wanted real, hands-on experience with dbt alongside the SQL/Python pipelines 
I already build at work not just reading about it. 
Built this to properly learn it: proper staging/mart structure, tests that run for real in CI,
dbt orchestrated through Airflow with monitoring and alerting, 
and a dashboard so the output's actually usable, not just sitting in a database.