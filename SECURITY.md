# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems. Report them privately
through GitHub: **Security → Report a vulnerability** on this repository. Include
what you found, how to reproduce it and its impact. We will acknowledge the
report and keep you informed while we fix it.

## Handling credentials

The benchmark handles provider tokens and ClickHouse credentials:

- Configuration files hold only the **names** of environment variables, never
  secrets. Keep secrets in `.env` (ignored by Git) or your deployment's secret store.
- Provider endpoint URLs can embed credentials. They are stored and shown only as
  a short hash; do not paste full URLs into issues or logs.
- Use separate ClickHouse users for migrations, subscribers and Grafana, with the
  privileges listed in [`docs/operations.md`](docs/operations.md) and
  [`sql/README.md`](sql/README.md).
