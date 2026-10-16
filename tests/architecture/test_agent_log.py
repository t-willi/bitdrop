"""The agent log (docs/agent-log.md) is what a new session reads first, so it must not rot.

These checks cannot tell whether the log is *complete*, but they catch the common decay: a new
package nobody wrote down, a file that was renamed while the log still points at it, a section that
was deleted.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LOG = ROOT / "docs" / "agent-log.md"
CLAUDE_MD = ROOT / "CLAUDE.md"

REQUIRED_SECTIONS = (
    "## 1. Start here",
    "## 2. Current state",
    "## 3. Decisions",
    "## 4. Session history",
    "## 5. Backlog",
    "## 6. Known limitations",
    "## 7. Working rules",
    "## 8. Gotchas",
    "## 9. Keeping this log current",
)
PATH_ROOTS = ("docs/", "tests/", "packages/", "scripts/", "benchmarks/", "examples/")


def log_text() -> str:
    assert LOG.is_file(), "docs/agent-log.md is missing"
    return LOG.read_text(encoding="utf-8")


def test_the_log_exists_and_says_when_it_was_last_updated() -> None:
    assert re.search(r"^Last updated: \d{4}-\d{2}-\d{2}$", log_text(), re.MULTILINE)


@pytest.mark.parametrize("section", REQUIRED_SECTIONS)
def test_every_required_section_is_present(section: str) -> None:
    assert section in log_text(), f"missing section {section!r}"


def test_every_workspace_package_is_described() -> None:
    text = log_text()
    packages = sorted(path.name for path in (ROOT / "packages").iterdir() if path.is_dir())

    undocumented = [name for name in packages if name not in text]

    assert undocumented == [], f"packages missing from the agent log: {undocumented}"


def test_every_file_the_log_points_at_exists() -> None:
    """Backticked repo paths such as `docs/principles.md` or `tests/e2e/` must exist."""
    missing = []
    for token in re.findall(r"`([^`\s]+)`", log_text()):
        path = token.split("::")[0].rstrip("/")
        if (
            "://" in token
            or any(char in path for char in "*<>{}")
            or not path.startswith(PATH_ROOTS)
        ):
            continue
        if not (ROOT / path).exists():
            missing.append(token)

    assert missing == [], f"the agent log mentions paths that do not exist: {missing}"


def test_claude_md_sends_new_sessions_to_the_log() -> None:
    assert CLAUDE_MD.is_file(), "CLAUDE.md is missing"
    assert "docs/agent-log.md" in CLAUDE_MD.read_text(encoding="utf-8")
