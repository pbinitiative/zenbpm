#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name=$1
  if [ -z "${!name:-}" ]; then
    echo "$name is required" >&2
    exit 1
  fi
}

configure_git() {
  git config user.name "zenbpm-release-bot"
  git config user.email "zenbpm-release-bot@users.noreply.github.com"
}

export_release_vars() {
  validate_version_format
  local branch kind backend_ref
  branch="release/$(plain_version)"
  kind=$(release_kind)
  backend_ref=$branch
  if [ "${BACKEND_TAG_EXISTS:-false}" = "true" ]; then
    backend_ref=$VERSION
  fi
  RELEASE_BRANCH=$branch
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "RELEASE_BRANCH=$branch" >> "$GITHUB_ENV"
    echo "RELEASE_KIND=$kind" >> "$GITHUB_ENV"
  fi
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "release-tag=$VERSION" >> "$GITHUB_OUTPUT"
    echo "release-branch=$branch" >> "$GITHUB_OUTPUT"
    echo "release-kind=$kind" >> "$GITHUB_OUTPUT"
    echo "backend-checkout-ref=$backend_ref" >> "$GITHUB_OUTPUT"
  fi
}

plain_version() {
  require_env VERSION
  printf '%s' "${VERSION#v}"
}

github_remote() {
  require_env GH_TOKEN
  require_env ORG
  local repo=$1
  printf 'https://x-access-token:%s@github.com/%s/%s.git' "$GH_TOKEN" "$ORG" "$repo"
}

bump_backend_versions() {
  require_env VERSION
  require_env BACKEND_REPO
  require_env RELEASE_BRANCH
  local version
  # Release tags may carry a prerelease suffix (v1.5.0-rc1); the tracked versions never do.
  version=$(plain_version)
  version=${version%%-*}

  printf 'v%s\n' "$version" > VERSION

  awk -v new_version="$version" '
    /^info:[[:space:]]*$/ { in_info = 1; print; next }
    in_info && /^[^[:space:]]/ { in_info = 0 }
    in_info && /^[[:space:]]+version:[[:space:]]*/ {
      sub(/version:[[:space:]]*.*/, "version: " new_version)
      updated = 1
    }
    { print }
    END { if (!updated) exit 1 }
  ' openapi/api.yaml > openapi/api.yaml.tmp
  mv openapi/api.yaml.tmp openapi/api.yaml

  configure_git
  if git diff --quiet -- VERSION openapi/api.yaml; then
    echo "Backend versions are already $version."
    return 0
  fi
  git add VERSION openapi/api.yaml
  git commit -m "chore: bump release version to $version"
  git push "$(github_remote "$BACKEND_REPO")" HEAD:"$RELEASE_BRANCH"
}

ensure_release_pr() {
  require_env ORG
  require_env BACKEND_REPO
  require_env RELEASE_BRANCH
  require_env VERSION
  local existing_pr

  existing_pr=$(gh pr list \
    --repo "$ORG/$BACKEND_REPO" \
    --head "$RELEASE_BRANCH" \
    --base main \
    --state open \
    --json number \
    --jq '.[0].number // empty')

  if [ -n "$existing_pr" ]; then
    echo "Release PR already exists: #$existing_pr"
    return 0
  fi

  gh pr create \
    --repo "$ORG/$BACKEND_REPO" \
    --head "$RELEASE_BRANCH" \
    --base main \
    --title "chore: release $VERSION" \
    --body "Merge $RELEASE_BRANCH back into main after the release is complete."
}

validate_version_format() {
  release_kind >/dev/null
}

release_kind() {
  require_env VERSION
  if [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'final\n'
    return 0
  fi
  if [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-rc(\.)?[1-9][0-9]*$ ]]; then
    printf 'rc\n'
    return 0
  fi
  echo "Invalid release version '$VERSION'. Expected vX.Y.Z, vX.Y.Z-rcN, or vX.Y.Z-rc.N" >&2
  exit 1
}

prepare_branch() {
  local repo=${1:?repo is required}
  require_env RELEASE_BRANCH
  configure_git
  if git ls-remote --exit-code --heads origin "$RELEASE_BRANCH" >/dev/null 2>&1; then
    git checkout "$RELEASE_BRANCH"
    git pull --ff-only origin "$RELEASE_BRANCH"
  else
    git checkout -B "$RELEASE_BRANCH"
  fi
  git push "$(github_remote "$repo")" "$RELEASE_BRANCH"
}

commit_frontend_release_branch() {
  require_env VERSION
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  configure_git
  if git diff --quiet; then
    echo "No frontend OpenAPI/generated changes to commit."
  else
    git add openapi/api.yaml src/base/openapi
    git commit -m "chore: prepare release $VERSION"
  fi
  git push "$(github_remote "$FRONTEND_REPO")" "$RELEASE_BRANCH"
}

tag_commit_sha() {
  local repo=${1:?repo is required}
  local ref type sha tag
  if ! ref=$(gh api "/repos/$ORG/$repo/git/ref/tags/$VERSION" 2>/dev/null); then
    return 1
  fi
  type=$(jq -r '.object.type' <<< "$ref")
  sha=$(jq -r '.object.sha' <<< "$ref")
  while [ "$type" = "tag" ]; do
    tag=$(gh api "/repos/$ORG/$repo/git/tags/$sha")
    type=$(jq -r '.object.type' <<< "$tag")
    sha=$(jq -r '.object.sha' <<< "$tag")
  done
  printf '%s\n' "$sha"
}

verify_tag_sha() {
  local repo=${1:?repo is required}
  local expected_sha=${2:?expected sha is required}
  local actual_sha
  if ! actual_sha=$(tag_commit_sha "$repo"); then
    echo "Could not resolve tag $VERSION in $ORG/$repo after creation" >&2
    return 1
  fi
  if [ "$actual_sha" != "$expected_sha" ]; then
    echo "Tag $VERSION in $ORG/$repo points to $actual_sha; expected tested commit $expected_sha" >&2
    return 1
  fi
  echo "Tag $VERSION already points to tested commit $expected_sha in $ORG/$repo."
}

ensure_release_tag() {
  local repo=${1:?repo is required}
  require_env ORG
  require_env VERSION
  require_env RELEASE_BRANCH
  local expected_sha actual_sha
  expected_sha=${EXPECTED_SHA:-}
  if [ -z "$expected_sha" ]; then
    expected_sha=$(gh api "/repos/$ORG/$repo/git/ref/heads/${RELEASE_BRANCH}" --jq .object.sha)
  fi
  if actual_sha=$(tag_commit_sha "$repo"); then
    if [ "$actual_sha" != "$expected_sha" ]; then
      echo "Tag $VERSION in $ORG/$repo points to $actual_sha; expected tested commit $expected_sha" >&2
      return 1
    fi
    echo "Tag $VERSION already points to tested commit $expected_sha in $ORG/$repo."
    return 0
  fi
  if gh api --silent --method POST "/repos/$ORG/$repo/git/refs" \
    -f ref="refs/tags/$VERSION" \
    -f sha="$expected_sha"; then
    echo "Created tag $VERSION at tested commit $expected_sha in $ORG/$repo."
    return 0
  fi
  # Another run may have created the ref between the read and write.
  verify_tag_sha "$repo" "$expected_sha"
}

dispatch_workflow() {
  local repo=${1:?repo is required}
  local workflow=${2:?workflow is required}
  local payload=${3:?payload is required}
  local response run_id run_url
  response=$(gh api --method POST "/repos/$ORG/$repo/actions/workflows/$workflow/dispatches" --input - <<< "$payload")
  run_id=$(jq -r '.workflow_run_id // empty' <<< "$response")
  run_url=$(jq -r '.html_url // empty' <<< "$response")
  if [[ ! "$run_id" =~ ^[0-9]+$ ]]; then
    echo "GitHub did not return a workflow run ID for $ORG/$repo/$workflow" >&2
    return 1
  fi
  echo "Dispatched $ORG/$repo/$workflow: ${run_url:-run $run_id}"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "run-id=$run_id" >> "$GITHUB_OUTPUT"
    echo "run-url=$run_url" >> "$GITHUB_OUTPUT"
  fi
}

dispatch_frontend_release() {
  require_env ORG
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  require_env VERSION
  local payload
  payload=$(jq -n \
    --arg ref "$RELEASE_BRANCH" \
    --arg version "$VERSION" \
    '{ref: $ref, return_run_details: true, inputs: {version: $version, checkout_ref: $version, notify: false}}')
  dispatch_workflow "$FRONTEND_REPO" release.yaml "$payload"
}

wait_frontend_release() {
  require_env FRONTEND_REPO
  wait_workflow_run "$FRONTEND_REPO"
}

dispatch_java_client_release() {
  require_env ORG
  require_env JAVA_CLIENT_REPO
  require_env VERSION
  local payload
  payload=$(jq -n --arg version "$VERSION" \
    '{ref: "main", return_run_details: true, inputs: {version: $version}}')
  dispatch_workflow "$JAVA_CLIENT_REPO" release.yaml "$payload"
}

wait_java_client_release() {
  require_env JAVA_CLIENT_REPO
  wait_workflow_run "$JAVA_CLIENT_REPO"
}

dispatch_docs_release() {
  require_env ORG
  require_env DOCS_REPO
  require_env VERSION
  local payload
  payload=$(jq -n --arg version "$VERSION" \
    '{ref: "main", return_run_details: true, inputs: {version: $version, backend_tag: $version}}')
  dispatch_workflow "$DOCS_REPO" version-docs.yaml "$payload"
}

wait_docs_release() {
  require_env DOCS_REPO
  wait_workflow_run "$DOCS_REPO"
}

wait_workflow_run() {
  require_env ORG
  local repo=${1:?repo is required}
  require_env RUN_ID
  if [[ ! "$RUN_ID" =~ ^[0-9]+$ ]]; then
    echo "RUN_ID must be a numeric GitHub Actions run ID" >&2
    return 1
  fi
  gh run watch "$RUN_ID" --repo "$ORG/$repo" --exit-status
}

notify_discord() {
  if [ -z "${DISCORD_WEBHOOK_URL:-}" ]; then
    echo "DISCORD_WEBHOOK_URL is not configured; skipping notification."
    return 0
  fi

  local content payload
  content="ZenBPM release ${VERSION:-unknown} ${RELEASE_RESULT:-unknown}. Workflow: ${WORKFLOW_URL:-unknown}"
  payload=$(printf '{"content":%s}' "$(printf '%s' "$content" | jq -R .)")
  curl -fsS -H 'Content-Type: application/json' -d "$payload" "$DISCORD_WEBHOOK_URL"
}

case "${1:-}" in
  export-release-vars) export_release_vars ;;
  bump-backend-versions) bump_backend_versions ;;
  ensure-release-pr) ensure_release_pr ;;
  validate-version-format) validate_version_format ;;
  release-kind) release_kind ;;
  prepare-backend-branch) require_env BACKEND_REPO; prepare_branch "$BACKEND_REPO" ;;
  prepare-frontend-branch) require_env FRONTEND_REPO; prepare_branch "$FRONTEND_REPO" ;;
  commit-frontend-release-branch) commit_frontend_release_branch ;;
  tag-backend) require_env BACKEND_REPO; ensure_release_tag "$BACKEND_REPO" ;;
  tag-frontend) require_env FRONTEND_REPO; ensure_release_tag "$FRONTEND_REPO" ;;
  dispatch-frontend-release) dispatch_frontend_release ;;
  wait-frontend-release) wait_frontend_release ;;
  dispatch-java-client-release) dispatch_java_client_release ;;
  wait-java-client-release) wait_java_client_release ;;
  dispatch-docs-release) dispatch_docs_release ;;
  wait-docs-release) wait_docs_release ;;
  notify-discord) notify_discord ;;
  *)
    echo "Usage: $0 <command>" >&2
    exit 2
    ;;
esac
