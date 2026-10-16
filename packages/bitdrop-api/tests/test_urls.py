"""Components are configured by URL, e.g. ``file:///dev/shm/bitdrop?max_value_bytes=1048576``."""

import pytest

from bitdrop_api.urls import parse_url


class TestParseUrl:
    def test_bare_url(self) -> None:
        parsed = parse_url("memory://", scheme="memory", allowed_params=())

        assert parsed.scheme == "memory"
        assert parsed.path == ""
        assert dict(parsed.params) == {}

    def test_absolute_path_is_preserved(self) -> None:
        parsed = parse_url("file:///dev/shm/bitdrop", scheme="file", allowed_params=())

        assert parsed.path == "/dev/shm/bitdrop"

    def test_query_parameters_are_collected(self) -> None:
        parsed = parse_url("memory://?a=1&b=two", scheme="memory", allowed_params=("a", "b"))

        assert dict(parsed.params) == {"a": "1", "b": "two"}

    @pytest.mark.parametrize(
        ("url", "problem"),
        [
            pytest.param("redis://", "scheme", id="wrong-scheme"),
            pytest.param("memory:no-slashes", "scheme", id="no-authority"),
            pytest.param("memory://somehost", "host", id="host-not-allowed"),
            pytest.param("memory://?colour=blue", "colour", id="unknown-parameter"),
            pytest.param("memory://?a=1&a=2", "a", id="repeated-parameter"),
        ],
    )
    def test_invalid_urls_raise_value_error_naming_the_problem(
        self, url: str, problem: str
    ) -> None:
        with pytest.raises(ValueError, match=problem):
            parse_url(url, scheme="memory", allowed_params=("a",))


class TestIntParam:
    def test_returns_the_integer(self) -> None:
        parsed = parse_url("memory://?n=42", scheme="memory", allowed_params=("n",))

        assert parsed.int_param("n") == 42

    def test_returns_none_when_absent(self) -> None:
        parsed = parse_url("memory://", scheme="memory", allowed_params=("n",))

        assert parsed.int_param("n") is None

    def test_raises_value_error_for_non_integers(self) -> None:
        parsed = parse_url("memory://?n=lots", scheme="memory", allowed_params=("n",))

        with pytest.raises(ValueError, match="'n'"):
            parsed.int_param("n")
