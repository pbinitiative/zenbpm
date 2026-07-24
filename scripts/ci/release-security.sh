#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-ghcr.io/pbinitiative/zenbpm}"
RELEASE_TAG="${RELEASE_TAG:-${GITHUB_REF_NAME:-}}"
TRIVY_VERSION="${TRIVY_VERSION:-0.69.3}"
TRIVY_ARCHIVE_SHA256="${TRIVY_ARCHIVE_SHA256:-1816b632dfe529869c740c0913e36bd1629cb7688bd5634f4a858c1d57c88b75}"
TRIVY_REPORT_DIR="${TRIVY_REPORT_DIR:-trivy-reports}"
TRIVY_INSTALL_DIR="${TRIVY_INSTALL_DIR:-/usr/local/bin}"

usage() {
  cat <<'EOF'
Usage: scripts/ci/release-security.sh <command>

Commands:
  install-trivy       Download and install the verified Trivy binary
  build-scan-image   Build the local Docker image scanned before publish
  scan               Run image, filesystem, and secret scans
  resolve-digest     Resolve the published image digest and write outputs
  summarize          Build trivy-reports/summary.json and write outputs
  notify-discord     Send the release scan summary to Discord

Common env:
  RELEASE_TAG         Release tag. Defaults to GITHUB_REF_NAME.
  IMAGE_NAME          Image name. Defaults to ghcr.io/pbinitiative/zenbpm.
  TRIVY_REPORT_DIR    Report directory. Defaults to trivy-reports.
EOF
}

require_release_tag() {
  if [ -z "$RELEASE_TAG" ]; then
    echo "RELEASE_TAG or GITHUB_REF_NAME is required" >&2
    exit 1
  fi
}

github_output() {
  local name="$1"
  local value="$2"

  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf '%s=%s\n' "$name" "$value" >> "$GITHUB_OUTPUT"
  else
    printf '%s=%s\n' "$name" "$value"
  fi
}

install_trivy() {
  local asset="trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz"
  local url="https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/${asset}"
  local tmpdir

  tmpdir=$(mktemp -d)
  trap "rm -rf '$tmpdir'" EXIT

  curl -fsSLo "${tmpdir}/${asset}" "$url"
  printf '%s  %s\n' "$TRIVY_ARCHIVE_SHA256" "${tmpdir}/${asset}" | sha256sum -c -
  tar -xzf "${tmpdir}/${asset}" -C "$tmpdir" trivy

  if [ -w "$TRIVY_INSTALL_DIR" ]; then
    install -m 0755 "${tmpdir}/trivy" "${TRIVY_INSTALL_DIR}/trivy"
  else
    sudo install -m 0755 "${tmpdir}/trivy" "${TRIVY_INSTALL_DIR}/trivy"
  fi

  "${TRIVY_INSTALL_DIR}/trivy" --version
  rm -rf "$tmpdir"
  trap - EXIT
}

build_scan_image() {
  require_release_tag

  make generate
  mkdir -p linux/amd64
  CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-s -w -X main.commit=$(git rev-parse --short=7 HEAD)" \
    -o linux/amd64/zenbpm \
    ./cmd/zenbpm

  docker buildx build \
    --load \
    --platform linux/amd64 \
    --tag "${IMAGE_NAME}:${RELEASE_TAG}" \
    .
}

run_trivy() {
  trivy "$@"
}

scan() {
  require_release_tag

  mkdir -p "$TRIVY_REPORT_DIR"

  local image_ref="${IMAGE_NAME}:${RELEASE_TAG}"
  local ignore_args=()
  if [ -f .trivyignore ]; then
    ignore_args+=(--ignorefile .trivyignore)
  fi

  run_trivy image --skip-version-check --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 0 \
    "${ignore_args[@]}" --format json --output "${TRIVY_REPORT_DIR}/image.json" "$image_ref"
  run_trivy image --skip-version-check --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 0 \
    "${ignore_args[@]}" --format sarif --output "${TRIVY_REPORT_DIR}/image.sarif" "$image_ref"
  run_trivy image --skip-version-check --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 0 \
    "${ignore_args[@]}" --format table --output "${TRIVY_REPORT_DIR}/image.table.txt" "$image_ref"

  run_trivy fs --skip-version-check --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 0 \
    "${ignore_args[@]}" --format json --output "${TRIVY_REPORT_DIR}/fs.json" .
  run_trivy fs --skip-version-check --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 0 \
    "${ignore_args[@]}" --format sarif --output "${TRIVY_REPORT_DIR}/fs.sarif" .
  run_trivy fs --skip-version-check --scanners vuln --severity HIGH,CRITICAL --ignore-unfixed --exit-code 0 \
    "${ignore_args[@]}" --format table --output "${TRIVY_REPORT_DIR}/fs.table.txt" .

  run_trivy fs --skip-version-check --scanners secret --exit-code 0 \
    --format json --output "${TRIVY_REPORT_DIR}/secrets.json" .
  run_trivy fs --skip-version-check --scanners secret --exit-code 0 \
    --format sarif --output "${TRIVY_REPORT_DIR}/secrets.sarif" .
  run_trivy fs --skip-version-check --scanners secret --exit-code 0 \
    --format table --output "${TRIVY_REPORT_DIR}/secrets.table.txt" .
}

resolve_digest() {
  require_release_tag

  mkdir -p "$TRIVY_REPORT_DIR"

  local image_ref="${IMAGE_NAME}:${RELEASE_TAG}"
  local digest=""
  local digest_json

  if digest_json=$(docker buildx imagetools inspect "$image_ref" --format '{{json .Manifest.Digest}}' 2>/dev/null); then
    digest=$(printf '%s' "$digest_json" | jq -r '.')
  fi

  local image_with_digest
  if [ -n "$digest" ] && [ "$digest" != "null" ]; then
    image_with_digest="${image_ref}@${digest}"
  else
    image_with_digest="${image_ref}@unknown"
  fi

  jq -n \
    --arg digest "$digest" \
    --arg image_ref "$image_ref" \
    --arg image_with_digest "$image_with_digest" \
    '{digest: $digest, image_ref: $image_ref, image_with_digest: $image_with_digest}' \
    > "${TRIVY_REPORT_DIR}/image-digest.json"

  github_output digest "$digest"
  github_output image_ref "$image_ref"
  github_output image_with_digest "$image_with_digest"
}

count_vulns() {
  local file="$1"
  local severity="$2"

  if [ -s "$file" ]; then
    jq --arg severity "$severity" '[.Results[]?.Vulnerabilities[]? | select(.Severity == $severity)] | length' "$file"
  else
    printf '0\n'
  fi
}

count_secrets() {
  local file="$1"

  if [ -s "$file" ]; then
    jq '[.Results[]?.Secrets[]?] | length' "$file"
  else
    printf '0\n'
  fi
}

summary_value() {
  local key="$1"
  local fallback="$2"

  if [ -s "${TRIVY_REPORT_DIR}/image-digest.json" ]; then
    jq -r --arg key "$key" --arg fallback "$fallback" '.[$key] // $fallback' "${TRIVY_REPORT_DIR}/image-digest.json"
  else
    printf '%s\n' "$fallback"
  fi
}

summarize() {
  require_release_tag

  mkdir -p "$TRIVY_REPORT_DIR"

  local image_ref="${IMAGE_REF:-}"
  local image_with_digest="${IMAGE_WITH_DIGEST:-}"

  if [ -z "$image_ref" ]; then
    image_ref=$(summary_value image_ref "${IMAGE_NAME}:${RELEASE_TAG}")
  fi
  if [ -z "$image_with_digest" ]; then
    image_with_digest=$(summary_value image_with_digest "${image_ref}@unknown")
  fi

  local image_high
  local image_critical
  local fs_high
  local fs_critical
  local secret_count
  local workflow_url
  local security_url

  image_high=$(count_vulns "${TRIVY_REPORT_DIR}/image.json" HIGH)
  image_critical=$(count_vulns "${TRIVY_REPORT_DIR}/image.json" CRITICAL)
  fs_high=$(count_vulns "${TRIVY_REPORT_DIR}/fs.json" HIGH)
  fs_critical=$(count_vulns "${TRIVY_REPORT_DIR}/fs.json" CRITICAL)
  secret_count=$(count_secrets "${TRIVY_REPORT_DIR}/secrets.json")
  workflow_url="${WORKFLOW_URL:-${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-pbinitiative/zenbpm}/actions/runs/${GITHUB_RUN_ID:-local}}"
  security_url="${SECURITY_URL:-${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-pbinitiative/zenbpm}/security/code-scanning}"

  jq -n \
    --arg tag "$RELEASE_TAG" \
    --arg image_ref "$image_ref" \
    --arg image_with_digest "$image_with_digest" \
    --arg workflow_url "$workflow_url" \
    --arg security_url "$security_url" \
    --argjson image_high "$image_high" \
    --argjson image_critical "$image_critical" \
    --argjson fs_high "$fs_high" \
    --argjson fs_critical "$fs_critical" \
    --argjson secret_count "$secret_count" \
    '{
      tag: $tag,
      image_ref: $image_ref,
      image_with_digest: $image_with_digest,
      image_vulnerabilities: {HIGH: $image_high, CRITICAL: $image_critical},
      filesystem_vulnerabilities: {HIGH: $fs_high, CRITICAL: $fs_critical},
      secrets: $secret_count,
      workflow_url: $workflow_url,
      security_url: $security_url
    }' > "${TRIVY_REPORT_DIR}/summary.json"

  github_output image_high "$image_high"
  github_output image_critical "$image_critical"
  github_output fs_high "$fs_high"
  github_output fs_critical "$fs_critical"
  github_output secret_count "$secret_count"
  github_output workflow_url "$workflow_url"
  github_output security_url "$security_url"
}

notify_discord() {
  local summary_file="${TRIVY_REPORT_DIR}/summary.json"

  if [ -z "${DISCORD_WEBHOOK_URL:-}" ]; then
    echo "DISCORD_WEBHOOK_URL is not configured; skipping Discord notification."
    return 0
  fi

  if [ ! -s "$summary_file" ]; then
    echo "${summary_file} does not exist; run summarize before notify-discord" >&2
    exit 1
  fi

  local payload
  payload=$(jq '{
    content: (
      "ZenBPM release " + .tag + " security scan\n" +
      "Image: " + .image_with_digest + "\n" +
      "Image vulns: HIGH=" + (.image_vulnerabilities.HIGH | tostring) + " CRITICAL=" + (.image_vulnerabilities.CRITICAL | tostring) + "\n" +
      "Filesystem vulns: HIGH=" + (.filesystem_vulnerabilities.HIGH | tostring) + " CRITICAL=" + (.filesystem_vulnerabilities.CRITICAL | tostring) + "\n" +
      "Secrets: " + (.secrets | tostring) + "\n" +
      "Workflow run: " + .workflow_url + "\n" +
      "Security tab: " + .security_url
    )
  }' "$summary_file")

  curl -fsS \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$DISCORD_WEBHOOK_URL"
}

command="${1:-}"
case "$command" in
  install-trivy)
    install_trivy
    ;;
  build-scan-image)
    build_scan_image
    ;;
  scan)
    scan
    ;;
  resolve-digest)
    resolve_digest
    ;;
  summarize)
    summarize
    ;;
  notify-discord)
    notify_discord
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
esac
