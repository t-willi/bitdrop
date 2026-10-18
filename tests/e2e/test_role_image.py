"""The role image (docker/Dockerfile) must build and start the command (needs Docker).

Every role of the queue workflow is this one image with a different command, so an image that builds
but cannot import its own packages breaks every container at once. Nothing else in the suite builds
it; the static checks of ``tests/architecture/test_queue_example.py`` cannot see this.
"""

import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = pytest.mark.docker

ROOT = Path(__file__).resolve().parents[2]
BUILD_TIMEOUT_SECONDS = 600


def _docker(*arguments: str, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *arguments],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


@pytest.fixture(scope="module")
def role_image() -> Iterator[str]:
    """Build the image once under a name of its own, and remove it afterwards."""
    try:
        reachable = _docker("info", timeout=30).returncode == 0
    except OSError, subprocess.TimeoutExpired:
        reachable = False
    if not reachable:
        pytest.skip("Docker is not available")
    tag = f"bitdrop-role-test:{uuid.uuid4().hex[:8]}"
    built = _docker(
        "build", "-f", "docker/Dockerfile", "-t", tag, str(ROOT), timeout=BUILD_TIMEOUT_SECONDS
    )
    if built.returncode != 0:
        tail = "\n".join((built.stdout + built.stderr).splitlines()[-30:])
        pytest.fail(f"the role image does not build:\n{tail}")
    yield tag
    _docker("rmi", "-f", tag)


def _components(image: str) -> dict[str, list[str]]:
    """What ``bitdrop components`` lists inside the image: kind to names."""
    ran = _docker("run", "--rm", image, "components")
    assert ran.returncode == 0, f"the command does not start:\n{ran.stderr[-1500:]}"
    listing: dict[str, list[str]] = {}
    for line in ran.stdout.splitlines():
        kind, _, names = line.partition(" ")
        listing[kind] = [name.strip() for name in names.split(",") if name.strip()]
    return listing


def test_the_command_starts_and_finds_the_adapters_of_the_queue_workflow(role_image: str) -> None:
    listing = _components(role_image)

    assert listing["sources"] == ["postgresql"]
    assert listing["destinations"] == ["postgresql"]
    assert listing["staging"] == ["redis"]
    assert listing["queues"] == ["redis"]
    assert listing["markers"] == ["redis"]


def test_the_image_has_no_display_toolkit(role_image: str) -> None:
    assert "tui" not in _components(role_image)["displays"]
