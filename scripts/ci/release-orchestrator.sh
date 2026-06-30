#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name=$1
  local -n value_ref=$name
  if [ -z "${value_ref:-}" ]; then
    echo "$name is required" >&2
    exit 1
  fi
}

configure_git() {
  git config user.name "zenbpm-release-bot"
  git config user.email "zenbpm-release-bot@users.noreply.github.com"
}

export_release_vars() {
  require_env RELEASE_TAG
  local branch
  branch="release/$(plain_version)"
  RELEASE_BRANCH=$branch
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "RELEASE_BRANCH=$branch" >> "$GITHUB_ENV"
  fi
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "release-tag=$RELEASE_TAG" >> "$GITHUB_OUTPUT"
    echo "release-branch=$branch" >> "$GITHUB_OUTPUT"
  fi
}

plain_version() {
  require_env VERSION
  printf '%s' "$VERSION"
}

github_remote() {
  require_env GH_TOKEN
  require_env ORG
  local repo=$1
  printf 'https://x-access-token:%s@github.com/%s/%s.git' "$GH_TOKEN" "$ORG" "$repo"
}

openapi_version() {
  awk '
    /^info:[[:space:]]*$/ { in_info = 1; next }
    in_info && /^[^[:space:]]/ { in_info = 0 }
    in_info && /^[[:space:]]+version:[[:space:]]*/ {
      value = $0
      sub(/^[[:space:]]+version:[[:space:]]*/, "", value)
      gsub(/^['"'"']|['"'"']$/, "", value)
      print value
      found = 1
      exit
    }
    END { if (!found) exit 1 }
  ' openapi/api.yaml
}

validate_openapi_version() {
  require_env VERSION
  local actual
  local expected
  expected=$(plain_version)
  if ! actual=$(openapi_version); then
    echo "openapi/api.yaml info.version was not found" >&2
    exit 1
  fi
  if [ "$actual" != "$expected" ]; then
    echo "OpenAPI version mismatch: expected $expected, got $actual" >&2
    exit 1
  fi
}

validate_version_format() {
  require_env VERSION
  require_env RELEASE_TAG
  if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "Invalid release version '$VERSION'. Expected SemVer without v prefix, for example 2.0.0 or 2.0.0-rc.1" >&2
    exit 1
  fi
  if [ "$RELEASE_TAG" != "v$VERSION" ]; then
    echo "Invalid release tag '$RELEASE_TAG'. Expected v$VERSION" >&2
    exit 1
  fi
}

validate_tags() {
  require_env ORG
  require_env RELEASE_TAG
  require_env BACKEND_REPO
  require_env FRONTEND_REPO
  require_env JAVA_CLIENT_REPO
  require_env DOCS_REPO
  for repo in "$BACKEND_REPO" "$FRONTEND_REPO" "$JAVA_CLIENT_REPO" "$DOCS_REPO"; do
    if gh api -H "Accept: application/vnd.github+json" "/repos/$ORG/$repo/git/ref/tags/$RELEASE_TAG" >/dev/null 2>&1; then
      echo "Tag $RELEASE_TAG already exists in $ORG/$repo" >&2
      exit 1
    fi
  done
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
  require_env RELEASE_TAG
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  configure_git
  if git diff --quiet; then
    echo "No frontend OpenAPI/generated changes to commit."
  else
    git add openapi/api.yaml src/base/openapi
    git commit -m "chore: prepare release $RELEASE_TAG"
  fi
  git push "$(github_remote "$FRONTEND_REPO")" "$RELEASE_BRANCH"
}

create_release_tag() {
  local repo=${1:?repo is required}
  require_env ORG
  require_env RELEASE_BRANCH
  require_env RELEASE_TAG
  local sha
  sha=$(gh api "/repos/$ORG/$repo/git/ref/heads/${RELEASE_BRANCH}" --jq .object.sha)
  gh api "/repos/$ORG/$repo/git/refs" \
    -f ref="refs/tags/$RELEASE_TAG" \
    -f sha="$sha"
}

dispatch_frontend_release() {
  require_env ORG
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  require_env VERSION
  require_env RELEASE_TAG
  gh workflow run release.yaml \
    --repo "$ORG/$FRONTEND_REPO" \
    --ref "$RELEASE_BRANCH" \
    -f version="$VERSION" \
    -f checkout_ref="$RELEASE_TAG"
}

wait_frontend_release() {
  require_env ORG
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  local run_id
  sleep 10
  run_id=$(gh run list --repo "$ORG/$FRONTEND_REPO" --workflow release.yaml --branch "$RELEASE_BRANCH" --json databaseId --jq '.[0].databaseId')
  if [ -z "$run_id" ] || [ "$run_id" = "null" ]; then
    echo "Could not find dispatched frontend release workflow run" >&2
    exit 1
  fi
  gh run watch "$run_id" --repo "$ORG/$FRONTEND_REPO" --exit-status
}

dispatch_java_client_release() {
  require_env ORG
  require_env JAVA_CLIENT_REPO
  require_env VERSION
  require_env RELEASE_TAG
  gh workflow run release.yaml \
    --repo "$ORG/$JAVA_CLIENT_REPO" \
    --ref main \
    -f version="$VERSION" \
    -f backend_tag="$RELEASE_TAG"
}

dispatch_docs_release() {
  require_env ORG
  require_env DOCS_REPO
  require_env VERSION
  require_env RELEASE_TAG
  gh workflow run version-docs.yaml \
    --repo "$ORG/$DOCS_REPO" \
    --ref main \
    -f version="$VERSION" \
    -f backend_tag="$RELEASE_TAG"
}

notify_discord() {
  if [ -z "${DISCORD_WEBHOOK_URL:-}" ]; then
    echo "DISCORD_WEBHOOK_URL is not configured; skipping notification."
    return 0
  fi

  local content payload
  content="ZenBPM release ${RELEASE_TAG:-unknown} ${RELEASE_RESULT:-unknown}. Workflow: ${WORKFLOW_URL:-unknown}"
  payload=$(printf '{"content":%s}' "$(printf '%s' "$content" | jq -R .)")
  curl -fsS -H 'Content-Type: application/json' -d "$payload" "$DISCORD_WEBHOOK_URL"
}

case "${1:-}" in
  export-release-vars) export_release_vars ;;
  validate-openapi-version) validate_openapi_version ;;
  validate-version-format) validate_version_format ;;
  validate-tags) validate_tags ;;
  prepare-backend-branch) require_env BACKEND_REPO; prepare_branch "$BACKEND_REPO" ;;
  prepare-frontend-branch) require_env FRONTEND_REPO; prepare_branch "$FRONTEND_REPO" ;;
  commit-frontend-release-branch) commit_frontend_release_branch ;;
  tag-backend) require_env BACKEND_REPO; create_release_tag "$BACKEND_REPO" ;;
  tag-frontend) require_env FRONTEND_REPO; create_release_tag "$FRONTEND_REPO" ;;
  dispatch-frontend-release) dispatch_frontend_release ;;
  wait-frontend-release) wait_frontend_release ;;
  dispatch-java-client-release) dispatch_java_client_release ;;
  dispatch-docs-release) dispatch_docs_release ;;
  notify-discord) notify_discord ;;
  *)
    echo "Usage: $0 <command>" >&2
    exit 2
    ;;
esac
