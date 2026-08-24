# Gosec Security Scans

## Run Locally

```bash
make sast
```

Reports are written to `gosec-reports/`:

- `gosec.sarif` — machine-readable SARIF report for tool integrations
- `gosec.html` — human-friendly HTML report, open in a browser for an easy-to-read view

To open the HTML report after the scan:

```bash
xdg-open gosec-reports/gosec.html   # Linux
open gosec-reports/gosec.html       # macOS
```

### Strict Mode (fail on findings)

By default `make sast` runs with `-no-fail` so the build does not break when issues are found. To run in strict mode and exit non-zero when findings are present:

```bash
make sast-strict
```

### Direct invocation

You can also invoke gosec directly (after running `make gosec` to install the binary):

```bash
bin/gosec -exclude-generated -no-fail -fmt html -out gosec-reports/gosec.html -stdout -verbose text ./...
```

## GitHub Code Scanning

`.github/workflows/gosec.yml` runs on every push and pull request to `main`, as well as on a daily schedule at `05:00 UTC`, and can be triggered manually via `workflow_dispatch`.

The workflow uploads `gosec-reports/gosec.sarif` using `github/codeql-action/upload-sarif` under the category `gosec`.

Findings appear in GitHub `Security` > `Code scanning alerts`. GitHub tracks alerts by tool, rule, category, and location, so the same finding is not duplicated on repeated runs. New gosec findings create new alerts; existing alerts remain open until gosec no longer reports them.

Full scan output is always available from the workflow artifact named `gosec-reports-<run_id>`.

## Why Gosec May Not Appear

Gosec findings appear in GitHub `Security` > `Code scanning alerts` only when all of these are true:

- The workflow actually runs the gosec scan.
- SARIF is uploaded with `github/codeql-action/upload-sarif`.
- The workflow has `security-events: write` permission.
- For private repositories, GitHub code scanning is enabled and the repository plan supports third-party SARIF upload.

