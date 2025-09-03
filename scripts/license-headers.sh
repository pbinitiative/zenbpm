#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
if [[ "$MODE" != "add" && "$MODE" != "check" ]]; then
  echo "Usage: $0 {add|check}"
  exit 2
fi

# ----- Paths -----
SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="${BIN_DIR:-$REPO_ROOT/bin}"
PATH="$BIN_DIR:$PATH"

# ----- Config -----
HEADER_FILE="${HEADER_FILE:-LICENSE-HEADER.txt}"
EXTENSIONS="${EXTENSIONS:-go sql}"      # Space-separated list of file extensions
EXTRA_EXCLUDES="${EXTRA_EXCLUDES:-}"   # e.g. "**/*_generated.go **/gen/**"

# ----- Ensure header exists -----
if [[ ! -f "$REPO_ROOT/$HEADER_FILE" && ! -f "$HEADER_FILE" ]]; then
  echo "ERROR: header template '$HEADER_FILE' not found (looked in $REPO_ROOT and CWD)."
  exit 1
fi
# Resolve to absolute path
if [[ -f "$REPO_ROOT/$HEADER_FILE" ]]; then
  HEADER_FILE="$REPO_ROOT/$HEADER_FILE"
fi

# ----- Ensure addlicense is available -----
if ! command -v addlicense >/dev/null 2>&1; then
  mkdir -p "$BIN_DIR"
  echo "Installing addlicense into $BIN_DIR ..."
  GOBIN="$BIN_DIR" go install github.com/google/addlicense@latest
fi

# ----- Collect candidate files -----
ALL_FILES=()
for ext in $EXTENSIONS; do
  mapfile -d '' -O "${#ALL_FILES[@]}" ALL_FILES < <(
    cd "$REPO_ROOT"
    find . -type f -name "*.$ext" \
      ! -path './vendor/*' \
      ! -path './third_party/*' \
      -print0
  )
done

# ----- Apply EXTRA_EXCLUDES (bash glob matching) -----
FILTERED_FILES=()
if [[ -n "$EXTRA_EXCLUDES" ]]; then
  for f in "${ALL_FILES[@]}"; do
    keep=true
    for glob in $EXTRA_EXCLUDES; do
      if [[ "${f#./}" == $glob ]]; then
        keep=false
        break
      fi
    done
    $keep && FILTERED_FILES+=("$f")
  done
else
  FILTERED_FILES=("${ALL_FILES[@]}")
fi

# ----- Exclude generated files (if "Code generated" in first 5 lines) -----
FINAL_FILES=()
for f in "${FILTERED_FILES[@]}"; do
  # Check for Go-style generated file markers (// Code generated)
  # and SQL-style generated file markers (-- Code generated)
  if head -n 5 "$REPO_ROOT/$f" | grep -qE '^[[:space:]]*(//|--)[[:space:]]*Code generated'; then
    continue
  fi
  FINAL_FILES+=("$REPO_ROOT/$f")
done

# ----- Nothing to do? -----
if [[ ${#FINAL_FILES[@]} -eq 0 ]]; then
  if [[ "$MODE" == "add" ]]; then
    echo "No non-generated files found to license (extensions: $EXTENSIONS)."
  else
    echo "No non-generated files found to check (extensions: $EXTENSIONS)."
  fi
  exit 0
fi

# ----- Run addlicense -----
if [[ "$MODE" == "check" ]]; then
  addlicense -check -f "$HEADER_FILE" "${FINAL_FILES[@]}"
else
  addlicense -f "$HEADER_FILE" "${FINAL_FILES[@]}"
fi
