# Contributing

Thanks for helping improve the benchmark. Issues and pull requests are welcome.

## Before you start

For anything beyond a small fix, open an issue first so we can agree on the
approach. Changes to what is measured, or how (matching, drops, wins, coverage),
change the meaning of published results: explain the reason and the effect on
existing data in the issue.

## Development setup

- Rust (see `rust-version` in `Cargo.toml`)
- Node.js 22 or newer, for the Grafana dashboards
- Python 3 and the [`clickhouse`](https://clickhouse.com/docs/install) CLI, for the
  dashboard SQL tests
- Optionally Docker, to run ClickHouse and Grafana locally (`docker compose up`)

## Checks

CI runs all of these; run the ones your change touches before opening a pull request.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test

npm --prefix grafana ci
npm --prefix grafana run check
npm --prefix grafana run build   # commit the regenerated dashboard JSON
npm --prefix grafana test        # dashboard SQL against clickhouse-local fixtures
```

`npm --prefix grafana run test:rollups` checks the minute summaries against a real
ClickHouse server. It needs `BENCH_CH_USER` and `BENCH_CH_PASSWORD` (and
`BENCH_CH_URL`, default `http://127.0.0.1:8123`), and creates and finally drops its
own `benchmark_rollup_test_*` database. With `docker compose up`, use
`BENCH_CH_USER=benchmark BENCH_CH_PASSWORD=benchmark`.

## Where things live

| Path | Contents |
| --- | --- |
| `src/stream.rs` | Yellowstone subscriptions: connect, stamp, decode, enqueue |
| `src/pipeline.rs`, `src/matcher.rs`, `src/forks.rs` | Per-stream matching, drops and certified coverage |
| `src/storage.rs`, `src/rollups.rs` | ClickHouse writer and minute summaries |
| `sql/migrations/` | Schema (Goose migrations) |
| `sql/rollups/` | Summary SQL, shared by the subscriber, the dashboards and the tests |
| `grafana/*.ts` | Dashboards (Grafana Foundation SDK) and their SQL in `grafana/queries.ts` |
| `grafana/tests/` | Dashboard SQL tests |
| `docs/` | Methodology, operations, dashboards and accuracy |

## Pull requests

- Keep changes focused, with tests for behavior changes.
- Schema changes need a new migration (see [`sql/README.md`](sql/README.md)).
- Dashboard changes: edit the TypeScript source, rebuild the JSON and commit both.
- Update the docs when behavior or configuration changes.

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
