"""Connection URLs: libpq understands the URL, bitdrop reads its own ``bitdrop_*`` options."""

import pytest

from bitdrop_connector_postgres._connection import parse_connection_url


class TestParseConnectionUrl:
    def test_a_plain_url_passes_through(self) -> None:
        settings = parse_connection_url("postgresql://u:p@host:5433/db")

        assert settings.uri == "postgresql://u:p@host:5433/db"
        assert settings.pool_size == 4

    def test_the_postgres_scheme_is_accepted_too(self) -> None:
        assert parse_connection_url("postgres://u@h/db").uri == "postgres://u@h/db"

    def test_libpq_options_are_kept(self) -> None:
        settings = parse_connection_url(
            "postgresql://h/db?sslmode=require&application_name=bitdrop"
        )

        assert "sslmode=require" in settings.uri
        assert "application_name=bitdrop" in settings.uri

    def test_bitdrop_options_are_read_and_removed_before_libpq_sees_them(self) -> None:
        settings = parse_connection_url("postgresql://h/db?sslmode=require&bitdrop_pool_size=9")

        assert settings.pool_size == 9
        assert "bitdrop" not in settings.uri
        assert "sslmode=require" in settings.uri

    def test_the_url_without_query_stays_free_of_a_question_mark(self) -> None:
        assert "?" not in parse_connection_url("postgresql://h/db?bitdrop_pool_size=2").uri

    @pytest.mark.parametrize("bad", ["0", "-1", "many"])
    def test_bad_pool_sizes_are_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError, match="bitdrop_pool_size"):
            parse_connection_url(f"postgresql://h/db?bitdrop_pool_size={bad}")

    def test_misspelled_bitdrop_options_are_rejected_not_ignored(self) -> None:
        with pytest.raises(ValueError, match=r"bitdrop_pool_sze.*bitdrop_pool_size"):
            parse_connection_url("postgresql://h/db?bitdrop_pool_sze=9")

    @pytest.mark.parametrize("url", ["mysql://h/db", "http://h", "h/db", ""])
    def test_other_schemes_are_rejected(self, url: str) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            parse_connection_url(url)

    def test_passwords_never_appear_in_the_redacted_form(self) -> None:
        settings = parse_connection_url("postgresql://alice:s3cr%40t@db.example.com:5432/shop")

        assert "s3cr" not in settings.redacted
        assert settings.redacted == "postgresql://alice:***@db.example.com:5432/shop"

    def test_urls_without_a_password_are_not_altered_when_redacted(self) -> None:
        assert parse_connection_url("postgresql://alice@h/db").redacted == "postgresql://alice@h/db"

    def test_passwords_in_the_query_string_are_redacted_too(self) -> None:
        settings = parse_connection_url("postgresql://alice@h/db?password=hunter2&sslmode=require")

        assert "hunter2" not in settings.redacted
        assert "sslmode=require" in settings.redacted
