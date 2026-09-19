#!/usr/bin/env bash
# One-shot: turn the wisdom-of-crowds project into a real GitHub repo,
# push it, and register it in the Databricks workspace as a Databricks Repo.
#
# Assumes:
#   - `gh` CLI is installed and authenticated (`gh auth status` returns green).
#   - The dbx-mcp venv (../dbx-mcp/.venv) exists so we can reuse databricks-sdk.
#   - .env in ../dbx-mcp/ has DATABRICKS_HOST + DATABRICKS_TOKEN.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DBX_MCP_DIR="$(cd "$PROJECT_DIR/../dbx-mcp" && pwd)"
REPO_NAME="wisdom-of-crowds"
VISIBILITY="public"     # a gift, .env is git-ignored; safe to be public.

cd "$PROJECT_DIR"

echo "─── publish wisdom-of-crowds ────────────────────────"
echo "Project:  $PROJECT_DIR"

# ── Preflight ────────────────────────────────────────────
command -v gh >/dev/null || { echo "ERROR: gh CLI not installed"; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "ERROR: gh not authenticated. Run 'gh auth login' first."; exit 1; }
GH_USER="$(gh api user --jq .login)"
echo "GitHub user: $GH_USER"

# ── Step 1 · git init + commit ───────────────────────────
if [ ! -d ".git" ]; then
    echo "[1/4] git init + first commit…"
    git init -b main >/dev/null
    git add .
    git -c user.name="$GH_USER" -c user.email="${GH_USER}@users.noreply.github.com" \
        commit -m "Initial commit: wisdom-of-crowds Galton replication + Databricks-ready notebook" >/dev/null
else
    echo "[1/4] git repo already exists — skipping init."
fi

# Safety check — the token file must never be in the index.
if git ls-files | grep -q "^\.env$"; then
    echo "ERROR: .env is tracked by git! Aborting before push." >&2
    exit 1
fi

# ── Step 2 · create GitHub repo (if missing) and push ────
if gh repo view "$GH_USER/$REPO_NAME" >/dev/null 2>&1; then
    echo "[2/4] GitHub repo $GH_USER/$REPO_NAME already exists."
    if ! git remote get-url origin >/dev/null 2>&1; then
        git remote add origin "https://github.com/$GH_USER/$REPO_NAME.git"
    fi
    git push -u origin main
else
    echo "[2/4] Creating $VISIBILITY GitHub repo $GH_USER/$REPO_NAME and pushing…"
    gh repo create "$REPO_NAME" --"$VISIBILITY" --source=. --remote=origin --push \
        --description "A faithful replication of Galton's 1907 wisdom-of-crowds experiment, wired for Databricks."
fi

REPO_URL="https://github.com/$GH_USER/$REPO_NAME"
echo "      $REPO_URL"

# ── Step 3 · Databricks Repo ─────────────────────────────
echo "[3/4] Registering as a Databricks Repo…"
# Reuse the dbx-mcp venv (already has databricks-sdk installed).
# shellcheck source=/dev/null
source "$DBX_MCP_DIR/.venv/bin/activate"

python - "$REPO_URL" <<'PY'
import os, sys, pathlib
from dotenv import load_dotenv
load_dotenv(pathlib.Path(os.environ["DBX_MCP_DIR"]) / ".env")
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import RepoInfo

url = sys.argv[1]
w = WorkspaceClient()
me = w.current_user.me().user_name
dbx_path = f"/Repos/{me}/wisdom-of-crowds"

# If a repo already exists at that path, print it and stop.
existing = None
for r in w.repos.list():
    if r.path == dbx_path:
        existing = r
        break

if existing:
    print(f"      Repo already exists at {dbx_path} (id={existing.id})")
    print(f"      URL      : {existing.url}")
    print(f"      Branch   : {existing.branch}")
    print(f"      HeadRev  : {existing.head_commit_id}")
else:
    created = w.repos.create(url=url, provider="gitHub", path=dbx_path)
    print(f"      Created Databricks Repo id={created.id}")
    print(f"      Workspace path: {created.path}")
    print(f"      Branch: {created.branch}")
PY

echo ""
echo "[4/4] Done."
echo "  GitHub  : $REPO_URL"
echo "  Databricks Repo path: /Repos/<you>/$REPO_NAME (open in workspace)"
