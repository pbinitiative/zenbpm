# Trivy Security Scans

## Run Locally

```bash
export RELEASE_TAG=local-trivy-$(git rev-parse --short HEAD)

scripts/ci/release-security.sh install-trivy
scripts/ci/release-security.sh build-scan-image
scripts/ci/release-security.sh scan
scripts/ci/release-security.sh summarize
```

Reports are written to `trivy-reports/`:

- `image.json`, `image.sarif`, `image.table.txt`
- `fs.json`, `fs.sarif`, `fs.table.txt`
- `secrets.json`, `secrets.sarif`, `secrets.table.txt`
- `summary.json`

Inspect the summary with:

```bash
jq . trivy-reports/summary.json
```

## GitHub Code Scanning

`.github/workflows/trivy-scan.yml` runs every morning at `06:00 UTC` and can also be started manually. It uploads Trivy SARIF with `github/codeql-action/upload-sarif@v4`.

GitHub code scanning is the stateful reporting channel. It tracks alerts by tool, rule, category, and location, so the same finding is not created as a new alert every day. New Trivy findings appear as new code scanning alerts. Existing findings remain open until Trivy no longer reports them.

When SARIF does not include `partialFingerprints`, `github/codeql-action/upload-sarif` calculates them during upload where possible. This is what prevents repeated uploads of the same finding from becoming duplicate daily alerts.

Full scan output is always available from the workflow artifact named `trivy-reports-<run_id>`.

## Why Trivy May Not Appear

Trivy findings appear in GitHub `Security` > `Code scanning alerts` only when all of these are true:

- The workflow actually runs the Trivy scan.
- SARIF is uploaded with `github/codeql-action/upload-sarif`.
- The workflow has `security-events: write` permission.
- For private repositories, GitHub code scanning is enabled and the repository plan supports third-party SARIF upload.

CodeQL `security-and-quality` is separate from Trivy. It is a CodeQL query suite, not the place where Trivy results are configured.
