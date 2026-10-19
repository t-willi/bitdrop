"""The job file: what a person writes to describe a transfer."""

from pathlib import Path

import pytest

from bitdrop_cli.jobfile import JobFile, JobFileError, load_job

GOOD = """
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

[metadata]
url = "memory://"
"""


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "job.toml"
    path.write_text(text)
    return path


def load(tmp_path: Path, text: str, environ: dict[str, str] | None = None) -> JobFile:
    return load_job(write(tmp_path, text), environ=environ or {})


class TestAGoodFile:
    def test_every_section_is_read(self, tmp_path: Path) -> None:
        job = load(tmp_path, GOOD)

        assert job.job.id == "orders-1"
        assert (job.source.url, job.source.table) == ("memory://?demo_rows=100", "demo")
        assert (job.destination.url, job.destination.table) == ("memory://", "orders")
        assert job.staging.url == job.require_metadata().url == "memory://"

    def test_tuning_is_optional_and_means_the_runner_defaults(self, tmp_path: Path) -> None:
        config = load(tmp_path, GOOD).transfer_config()

        assert (config.job_id, config.source_table, config.destination_table) == (
            "orders-1",
            "demo",
            "orders",
        )
        assert (config.extractors, config.loaders, config.lease_seconds) == (4, 4, 60.0)
        assert config.split_column is None

    def test_tuning_values_reach_the_runner(self, tmp_path: Path) -> None:
        text = GOOD + (
            "\n[tuning]\nextractors = 8\nloaders = 2\nchunk_mib = 16\nlease_seconds = 30\n"
            "max_attempts = 5\nchunk_timeout = 120\nbackpressure_timeout = 90\n"
        )

        config = load(tmp_path, text).transfer_config()

        assert (config.extractors, config.loaders) == (8, 2)
        assert config.target_chunk_bytes == 16 * 1024 * 1024
        assert (config.lease_seconds, config.max_attempts) == (30.0, 5)
        assert (config.chunk_timeout, config.backpressure_timeout) == (120.0, 90.0)

    def test_a_split_column_is_passed_on(self, tmp_path: Path) -> None:
        text = GOOD.replace('table = "demo"', 'table = "demo"\nsplit_column = "id"')

        assert load(tmp_path, text).transfer_config().split_column == "id"

    def test_command_line_overrides_win(self, tmp_path: Path) -> None:
        config = load(tmp_path, GOOD).transfer_config(job_id="again", extractors=2, loaders=3)

        assert (config.job_id, config.extractors, config.loaders) == ("again", 2, 3)

    def test_endpoints_are_listed_without_passwords(self, tmp_path: Path) -> None:
        text = GOOD.replace("memory://?demo_rows=100", "db://alice:s3cret@host/shop")

        endpoints = load(tmp_path, text).endpoints()

        assert endpoints["source"] == "db://alice:***@host/shop"
        assert set(endpoints) == {"source", "destination", "staging", "metadata"}


class TestEnvironmentVariables:
    def test_a_url_may_name_a_variable_so_the_password_stays_out_of_the_file(
        self, tmp_path: Path
    ) -> None:
        text = GOOD.replace("memory://?demo_rows=100", "${SOURCE_URL}")

        job = load(tmp_path, text, {"SOURCE_URL": "db://alice:s3cret@host/shop"})

        assert job.source.url == "db://alice:s3cret@host/shop"

    def test_a_variable_may_be_part_of_a_url(self, tmp_path: Path) -> None:
        text = GOOD.replace("memory://?demo_rows=100", "db://alice:${DB_PASSWORD}@host/shop")

        job = load(tmp_path, text, {"DB_PASSWORD": "s3cret"})

        assert job.source.url == "db://alice:s3cret@host/shop"

    def test_an_unset_variable_is_named_in_the_error(self, tmp_path: Path) -> None:
        text = GOOD.replace("memory://?demo_rows=100", "${SOURCE_URL}")

        with pytest.raises(JobFileError, match="SOURCE_URL"):
            load(tmp_path, text, {})

    def test_variables_are_not_expanded_outside_urls(self, tmp_path: Path) -> None:
        text = GOOD.replace('table = "orders"', 'table = "${HOME}"')

        assert load(tmp_path, text, {"HOME": "/root"}).destination.table == "${HOME}"


class TestMistakes:
    def test_a_missing_file_is_reported_by_name(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match=r"nope\.toml"):
            load_job(tmp_path / "nope.toml", environ={})

    def test_broken_toml_is_reported_as_such(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match="TOML"):
            load(tmp_path, "[job\nid = ")

    def test_a_missing_required_field_names_the_section_and_the_field(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match=r"source\.table"):
            load(tmp_path, GOOD.replace('table = "demo"', ""))

    def test_a_missing_section_is_named(self, tmp_path: Path) -> None:
        text = GOOD.split("[staging]")[0]

        with pytest.raises(JobFileError, match=r"staging.*(required|missing)"):
            load(tmp_path, text)

    def test_a_misspelled_key_is_an_error_not_a_silent_default(self, tmp_path: Path) -> None:
        text = GOOD + "\n[tuning]\nextractor = 8\n"

        with pytest.raises(JobFileError, match=r"tuning\.extractor"):
            load(tmp_path, text)

    def test_a_misspelled_section_is_an_error_too(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match="tunning"):
            load(tmp_path, GOOD + "\n[tunning]\nextractors = 8\n")

    @pytest.mark.parametrize(
        "line",
        [
            "extractors = 0",
            "loaders = -1",
            "chunk_mib = 0",
            "lease_seconds = 0",
            "max_attempts = 0",
        ],
    )
    def test_nonsense_numbers_are_rejected(self, tmp_path: Path, line: str) -> None:
        with pytest.raises(JobFileError, match=line.split(" ")[0]):
            load(tmp_path, GOOD + f"\n[tuning]\n{line}\n")

    def test_the_job_id_follows_the_key_rules(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match=r"job\.id"):
            load(tmp_path, GOOD.replace("orders-1", "has/slash"))

    def test_an_overlong_job_id_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(JobFileError, match=r"job\.id"):
            load(tmp_path, GOOD.replace("orders-1", "j" * 65))

    def test_every_problem_is_listed_at_once(self, tmp_path: Path) -> None:
        text = GOOD.replace('table = "demo"', "").replace('table = "orders"', "")

        with pytest.raises(JobFileError) as caught:
            load(tmp_path, text)

        assert "source.table" in str(caught.value)
        assert "destination.table" in str(caught.value)


class TestSecretsStayOutOfErrors:
    def test_a_problem_elsewhere_does_not_print_the_url_with_its_password(
        self, tmp_path: Path
    ) -> None:
        text = GOOD.replace("memory://?demo_rows=100", "${SOURCE_URL}").replace(
            'table = "orders"', "table = 5"
        )

        with pytest.raises(JobFileError) as caught:
            load(tmp_path, text, {"SOURCE_URL": "db://alice:s3cret@host/shop"})

        assert "s3cret" not in str(caught.value)

    def test_the_value_of_a_misspelled_key_is_not_echoed(self, tmp_path: Path) -> None:
        """The likeliest leak: a password typed under a key that does not exist."""
        text = GOOD.replace('table = "demo"', 'table = "demo"\npasword = "s3cret"')

        with pytest.raises(JobFileError) as caught:
            load(tmp_path, text)

        assert "source.pasword" in str(caught.value)
        assert "s3cret" not in str(caught.value)

    def test_a_url_of_the_wrong_type_is_not_echoed(self, tmp_path: Path) -> None:
        text = GOOD.replace('url = "memory://?demo_rows=100"', "url = 12345")

        with pytest.raises(JobFileError) as caught:
            load(tmp_path, text)

        assert "12345" not in str(caught.value)
