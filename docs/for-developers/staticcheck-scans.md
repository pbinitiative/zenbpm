# Staticcheck Scans

## Run Locally

```bash
make staticcheck
```

This runs [staticcheck](https://staticcheck.dev/) against the entire codebase, converts the JSON output to SARIF, and generates a human-friendly HTML report.

Reports are written to `staticcheck-reports/`:

- `staticcheck.json` — raw tool output in JSON format
- `staticcheck.sarif` — machine-readable SARIF report for tool integrations
- `staticcheck.html` — human-friendly HTML report, open in a browser for an easy-to-read view

To open the HTML report after the scan:

```bash
xdg-open staticcheck-reports/staticcheck.html   # Linux
open staticcheck-reports/staticcheck.html       # macOS
```

### Run all static analysis tools at once

To run staticcheck together with errcheck and revive in a single command:

```bash
make go-static-analysis
```

### Direct invocation

`make staticcheck` installs the binary (if needed) and runs the full report. To invoke staticcheck directly instead:

```bash
# Run and capture JSON output
bin/staticcheck -f=json ./... > staticcheck-reports/staticcheck.json

# Convert JSON to SARIF
python3 scripts/ci/go_tool_report_to_sarif.py staticcheck \
    staticcheck-reports/staticcheck.json \
    staticcheck-reports/staticcheck.sarif

# Convert SARIF to HTML
python3 scripts/ci/sarif_to_html.py \
    staticcheck-reports/staticcheck.sarif \
    staticcheck-reports/staticcheck.html
```

## GitHub Code Scanning

`.github/workflows/go-static-analysis.yml` runs staticcheck (alongside errcheck and revive) on every push and pull request to `main`, on a weekly schedule (Tuesdays at `05:12 UTC`), and can be triggered manually via `workflow_dispatch`.

The workflow uploads `staticcheck-reports/staticcheck.sarif` using `github/codeql-action/upload-sarif` under the category `staticcheck`.

Findings appear in GitHub `Security` > `Code scanning alerts`. GitHub tracks alerts by tool, rule, category, and location, so the same finding is not duplicated on repeated runs. New staticcheck findings create new alerts; existing alerts remain open until staticcheck no longer reports them.

Full scan output is always available from the workflow artifact named `staticcheck-reports-<run_id>`.

## Why Staticcheck May Not Appear

Staticcheck findings appear in GitHub `Security` > `Code scanning alerts` only when all of these are true:

- The workflow actually runs the staticcheck scan.
- SARIF is uploaded with `github/codeql-action/upload-sarif`.
- The workflow has `security-events: write` permission.
- For private repositories, GitHub code scanning is enabled and the repository plan supports third-party SARIF upload.

