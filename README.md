# Solana Streaming Benchmark

Compares Yellowstone gRPC providers by when the same Solana events (accounts, transactions, slots) reach your subscriber: who delivers first, by how much, and who drops events. Results go to ClickHouse and Grafana.

> **Disclosure:** Built and maintained by Helius, one of the providers it can measure. Results favor providers close to the subscriber, so compare from more than one location.

## Quick start

Needs Rust, Docker and Yellowstone gRPC endpoints.

```sh
docker compose up -d   # ClickHouse :8123, Grafana :3000 (override with CLICKHOUSE_PORT, GRAFANA_PORT)
cp config.example.yaml config.local.yaml   # set providers, filters, location; clickhouse.url: http://localhost:8123
cp .env.example .env                       # BENCH_CH_USER=benchmark, BENCH_CH_PASSWORD=benchmark, provider tokens
cargo run --release -- --config config.local.yaml
```

Then open <http://localhost:3000>. For production deployments, see [Operations](docs/operations.md).

## Docs

[Methodology](docs/methodology.md) · [Accuracy](docs/accuracy.md) · [Operations](docs/operations.md) · [Dashboards](docs/dashboards.md) · [Schema](sql/README.md) · [Example queries](sql/reports.sql) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

Licensed under [Apache-2.0](LICENSE).
