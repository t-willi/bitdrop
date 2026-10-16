"""``redact_url`` makes any URL safe to print, log, and put in error messages."""

import pytest

from bitdrop_api.urls import parse_url, redact_url


class TestRedactUrl:
    def test_the_password_is_replaced(self) -> None:
        assert (
            redact_url("postgresql://alice:s3cr3t@db.example.com:5432/shop")
            == "postgresql://alice:***@db.example.com:5432/shop"
        )

    def test_a_password_without_a_user_is_replaced_too(self) -> None:
        assert redact_url("redis://:hunter2@h:6379/0") == "redis://:***@h:6379/0"

    def test_a_password_in_the_query_is_replaced_and_other_options_stay(self) -> None:
        redacted = redact_url("postgresql://h/db?password=hunter2&sslmode=require")

        assert "hunter2" not in redacted
        assert "sslmode=require" in redacted

    @pytest.mark.parametrize(
        "url",
        [
            "memory://",
            "memory://?max_bytes=1000",
            "file:///dev/shm/bitdrop?locality=pod",
            "postgresql://alice@h/db",
            "redis://h:6379/0",
        ],
    )
    def test_urls_without_a_password_are_not_altered(self, url: str) -> None:
        assert redact_url(url) == url

    @pytest.mark.parametrize("url", ["postgresql://u:secret@[::1/db", "not a url:secret"])
    def test_a_url_that_cannot_be_read_is_never_echoed(self, url: str) -> None:
        redacted = redact_url(url)

        assert "secret" not in redacted


class TestErrorsNeverEchoSecrets:
    """A mistyped URL is exactly when a password is on screen; the error must not repeat it."""

    @pytest.mark.parametrize(
        ("url", "scheme", "allowed"),
        [
            pytest.param("memory://user:s3cret@host", "memory", (), id="host-with-password"),
            pytest.param("redis://user:s3cret@host", "memory", (), id="wrong-scheme"),
            pytest.param(
                "file:///p?password=s3cret&password=other", "file", ("password",), id="repeated"
            ),
            pytest.param("memory://?password=s3cret", "memory", (), id="unknown-parameter"),
        ],
    )
    def test_component_url_errors_hide_passwords(
        self, url: str, scheme: str, allowed: tuple[str, ...]
    ) -> None:
        with pytest.raises(ValueError, match=scheme) as caught:
            parse_url(url, scheme=scheme, allowed_params=allowed)

        assert "s3cret" not in str(caught.value)
