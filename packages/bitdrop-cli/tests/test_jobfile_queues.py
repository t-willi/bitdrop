"""The job file of the queue workflow: queues and markers, and no metadata store."""

from pathlib import Path

import pytest

from bitdrop_cli.jobfile import JobFile, JobFileError, load_job

QUEUE_JOB = """
[job]
id = "orders-1"

[source]
url = "memory://?demo_rows=100"
table = "demo"

[destination]
url = "memory://"
table = "orders"

[staging]
url = "memory://"

[queues]
work = "memory://"
files = "memory://"

[markers]
url = "memory://"
"""

MASTER_JOB = """
[job]
id = "orders-1"

[source]
url = "memory://"
table = "demo"

[destination]
url = "memory://"
table = "orders"

[staging]
url = "memory://"

[metadata]
url = "memory://"
"""


def load(tmp_path: Path, text: str, environ: dict[str, str] | None = None) -> JobFile:
    path = tmp_path / "job.toml"
    path.write_text(text)
    return load_job(path, environ=environ or {})


class TestAQueueJob:
    def test_it_needs_no_metadata_store(self, tmp_path: Path) -> None:
        job = load(tmp_path, QUEUE_JOB)

        assert job.metadata is None
        queues = job.require_queues()
        assert (queues.work, queues.files) == ("memory://", "memory://")
        assert job.require_markers().url == "memory://"

    def test_the_urls_of_the_queues_may_name_environment_variables(self, tmp_path: Path) -> None:
        text = QUEUE_JOB.replace(
            'work = "memory://"', 'work = "${QUEUE_URL}?bitdrop_queue=work"'
        ).replace('files = "memory://"', 'files = "${QUEUE_URL}?bitdrop_queue=files"')

        job = load(tmp_path, text, {"QUEUE_URL": "redis://redis:6379/0"})

        queues = job.require_queues()
        assert queues.work == "redis://redis:6379/0?bitdrop_queue=work"
        assert queues.files == "redis://redis:6379/0?bitdrop_queue=files"

    def test_the_endpoints_include_the_queues_and_markers_with_passwords_hidden(
        self, tmp_path: Path
    ) -> None:
        text = QUEUE_JOB.replace('work = "memory://"', 'work = "redis://:hunter2@h/0"').replace(
            '[markers]\nurl = "memory://"', '[markers]\nurl = "redis://:hunter2@h/0"'
        )

        endpoints = load(tmp_path, text).endpoints()

        assert set(endpoints) == {"source", "destination", "staging", "work", "files", "markers"}
        assert "hunter2" not in " ".join(endpoints.values())

    def test_a_misspelled_key_in_the_queues_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match=r"queues\.wrok"):
            load(tmp_path, QUEUE_JOB.replace("work =", "wrok ="))

    def test_both_queues_are_needed(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match=r"queues\.files"):
            load(tmp_path, QUEUE_JOB.replace('files = "memory://"\n', ""))


class TestWhatEachWorkflowNeeds:
    def test_a_master_job_has_no_queues_and_says_which_section_is_missing(
        self, tmp_path: Path
    ) -> None:
        job = load(tmp_path, MASTER_JOB)

        assert job.queues is None
        with pytest.raises(JobFileError, match=r"\[queues\]"):
            job.require_queues()
        with pytest.raises(JobFileError, match=r"\[markers\]"):
            job.require_markers()

    def test_a_queue_job_says_that_the_master_runner_needs_a_metadata_store(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(JobFileError, match=r"\[metadata\]"):
            load(tmp_path, QUEUE_JOB).require_metadata()

    def test_a_master_job_still_gives_its_metadata_store(self, tmp_path: Path) -> None:
        assert load(tmp_path, MASTER_JOB).require_metadata().url == "memory://"

    def test_a_file_with_neither_a_metadata_store_nor_queues_is_refused(
        self, tmp_path: Path
    ) -> None:
        text = QUEUE_JOB.replace('[queues]\nwork = "memory://"\nfiles = "memory://"\n', "")

        with pytest.raises(JobFileError, match=r"\[metadata\] or \[queues\]"):
            load(tmp_path, text)
