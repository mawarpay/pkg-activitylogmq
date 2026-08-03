# Contributing to pkg-activitylogmq

Thanks for contributing. This module is a shared library used by many IlonaPay services — keep changes small, documented, and backward compatible unless a major version bump is intentional.

## Development setup

Requires **Go 1.26+**.

```bash
git clone https://github.com/mawarpay/pkg-activitylogmq.git
cd pkg-activitylogmq
go test ./...
```

No Docker, broker, or network is required for unit tests.

## Workflow

1. Open an issue (optional but preferred for larger changes) describing the problem or proposal.
2. Fork and create a branch from `main`.
3. Make your change — see [Scope](#scope) and [API stability](#api-stability).
4. Add or update tests and GoDoc.
5. Run the local checks below.
6. Open a pull request against `main`. CI runs `go test ./...` via [`.github/workflows/test.yml`](./.github/workflows/test.yml).

## Local checks

```bash
go build ./...
go vet ./...
go test ./...
```

Prefer `gofmt` (or `go fmt ./...`) before committing.

## Scope

This repo is a **library only**:

- Do add config, broker adapters, messaging helpers, and the HTTP client for `activity-log-service`.
- Do not add HTTP/gRPC servers, routes, database access, or Compose services here.
- Keep dependencies lean (Watermill adapters and existing shared packages). Avoid heavyweight new deps without discussion.

Product requirements and known risks: [PRD.md](./PRD.md). Roadmap: [PLAN.md](./PLAN.md).

## API stability

- Prefer adding optional fields and functions over renaming or removing exports.
- Changing JSON tags or required fields on `clients.CreateBody` is a breaking change for every producer and for in-flight queue messages — coordinate with `activity-log-service` and consuming services first.
- Document every new exported symbol with GoDoc. Add an `Example…` in an `example_test.go` when the API is non-obvious.
- Update [README.md](./README.md) (env tables, usage) when the public surface or configuration changes.

## Testing guidelines

Unit tests must not require a live broker or real network:

- Prefer fakes for Watermill `message.Publisher` / `message.Subscriber` (see `messaging/activity_log_test.go`).
- Prefer `httptest` for `clients.ActivityLogClient`.
- Use `t.Setenv` for env-driven config; clear unrelated broker vars so auto-detect does not flip the broker unexpectedly.
- Reset package-level messaging state (`publisher`, `subscriber`, `router`, `brokerCfg`, `initOnce`) between tests when touching `messaging`.

## Commit and PR expectations

- Use clear, focused commits. Explain *why* in the PR description.
- Do not commit secrets, `.env` files, PEMs, or credentials.
- Do not force-push shared branches or skip CI unless maintainers ask.

## Monorepo consumers

This repository is also consumed as a Git submodule under IlonaPay (`api-service/pkg/activitylogmq`). After merging here:

1. Tag a release if consumers pin versions.
2. Bump the module version (or submodule pointer) in dependent services.
3. Run those services' tests.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](./LICENSE).
