# CodeQL Security Scans

## Run Locally

```bash
make codeql
```

This builds a CodeQL database from the Go source, runs the `go-security-extended` query suite against it, and converts the results to HTML.

Reports are written to `codeql-reports/`:

- `codeql.sarif` — machine-readable SARIF report for tool integrations
- `codeql.html` — human-friendly HTML report, open in a browser for an easy-to-read view
- `codeql-db/` — the CodeQL database created during the scan (can be reused for further analysis)

To open the HTML report after the scan:

```bash
xdg-open codeql-reports/codeql.html   # Linux
open codeql-reports/codeql.html       # macOS
```

### Prerequisites

The `make codeql` target will automatically download the CodeQL CLI if it is not already present in `bin/`. The download requires an internet connection and may take a few minutes the first time.

### Direct invocation

After running `make codeql-cli` to install the CLI, you can invoke CodeQL directly:

```bash
# Create the database
bin/codeql/codeql database create codeql-reports/codeql-db \
    --language=go \
    --build-mode=autobuild \
    --overwrite

# Run the analysis
bin/codeql/codeql database analyze codeql-reports/codeql-db \
    --download \
    codeql/go-queries:codeql-suites/go-security-extended.qls \
    --format=sarif-latest \
    --output=codeql-reports/codeql.sarif

# Convert to HTML
python3 scripts/ci/sarif_to_html.py codeql-reports/codeql.sarif codeql-reports/codeql.html
```

Inspect the SARIF report with:

```bash
jq . codeql-reports/codeql.sarif
```

## GitHub Code Scanning

`.github/workflows/codeql-analysis.yml` runs on every push and pull request to `main`, as well as on a weekly schedule (Fridays at `05:24 UTC`).

The workflow uses the hosted `github/codeql-action` to initialize CodeQL, perform the analysis with `autobuild`, and upload results. Findings appear directly in GitHub `Security` > `Code scanning alerts` under the category `/language:go`.

GitHub tracks alerts by tool, rule, category, and location so the same finding is not duplicated on repeated runs. New CodeQL findings create new alerts; existing alerts remain open until CodeQL no longer reports them.

Full SARIF output is always available from the workflow artifact named `codeql-reports-<run_id>`.

## Why CodeQL May Not Appear

CodeQL findings appear in GitHub `Security` > `Code scanning alerts` only when all of these are true:

- The workflow actually runs and the analysis step completes successfully.
- The workflow has `security-events: write` permission.
- For private repositories, GitHub code scanning is enabled and the repository plan supports CodeQL.

CodeQL `security-extended` (used here) is a broader superset of the default `security` query suite and will surface more potential findings. It is distinct from the `security-and-quality` suite; that suite includes additional code-quality queries and is not used in this workflow.

