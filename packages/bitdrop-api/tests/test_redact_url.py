"""``redact_url`` makes any URL safe to print, log, and put in error messages."""

import pytest

from bitdrop_api.urls import redact_url


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
