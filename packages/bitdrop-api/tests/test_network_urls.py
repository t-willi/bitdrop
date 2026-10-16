"""Connection URLs of network services: the client library gets the URL, bitdrop its own options."""

import pytest

from bitdrop_api.urls import parse_network_url


def parse(url: str, **kwargs: object):  # type: ignore[no-untyped-def]
    defaults: dict[str, object] = {
        "schemes": ("redis",),
        "options": ("bitdrop_pool", "bitdrop_name"),
    }
    return parse_network_url(url, **{**defaults, **kwargs})  # type: ignore[arg-type]


class TestParseNetworkUrl:
    def test_a_plain_url_passes_through_unchanged(self) -> None:
        parsed = parse("redis://u:p@host:6380/2")

        assert parsed.url == "redis://u:p@host:6380/2"
        assert dict(parsed.options) == {}

    def test_client_options_are_kept(self) -> None:
        parsed = parse("redis://h/0?socket_timeout=5&health_check_interval=30")

        assert "socket_timeout=5" in parsed.url
        assert "health_check_interval=30" in parsed.url

    def test_bitdrop_options_are_collected_and_removed_from_the_client_url(self) -> None:
        parsed = parse("redis://h/0?socket_timeout=5&bitdrop_pool=9&bitdrop_name=x")

        assert dict(parsed.options) == {"bitdrop_pool": "9", "bitdrop_name": "x"}
        assert "bitdrop" not in parsed.url
        assert "socket_timeout=5" in parsed.url

    def test_a_url_left_without_options_has_no_question_mark(self) -> None:
        assert "?" not in parse("redis://h/0?bitdrop_pool=2").url

    def test_values_with_spaces_stay_percent_encoded_for_the_client(self) -> None:
        parsed = parse("redis://h/0?client_name=my%20app")

        assert "client_name=my%20app" in parsed.url

    def test_misspelled_bitdrop_options_are_rejected_not_ignored(self) -> None:
        with pytest.raises(ValueError, match=r"bitdrop_pol.*bitdrop_pool"):
            parse("redis://h/0?bitdrop_pol=9")

    @pytest.mark.parametrize("url", ["mysql://h/db", "http://h", "h/db", ""])
    def test_other_schemes_are_rejected(self, url: str) -> None:
        with pytest.raises(ValueError, match="redis"):
            parse(url)

    def test_several_schemes_may_be_allowed(self) -> None:
        assert parse("postgres://h/db", schemes=("postgresql", "postgres")).url == "postgres://h/db"

    def test_passwords_never_appear_in_the_redacted_form(self) -> None:
        parsed = parse("redis://alice:s3cr%40t@cache.example.com:6379/0")

        assert "s3cr" not in parsed.redacted
        assert parsed.redacted == "redis://alice:***@cache.example.com:6379/0"

    def test_a_password_without_a_user_is_redacted_too(self) -> None:
        assert parse("redis://:hunter2@h:6379/0").redacted == "redis://:***@h:6379/0"

    def test_urls_without_a_password_are_not_altered_when_redacted(self) -> None:
        assert parse("redis://alice@h/0").redacted == "redis://alice@h/0"
        assert parse("redis://h:6379/0").redacted == "redis://h:6379/0"

    def test_passwords_in_the_query_string_are_redacted_too(self) -> None:
        parsed = parse("redis://h/0?password=hunter2&socket_timeout=5")

        assert "hunter2" not in parsed.redacted
        assert "socket_timeout=5" in parsed.redacted


class TestIntOption:
    def test_returns_the_value(self) -> None:
        assert parse("redis://h/0?bitdrop_pool=9").int_option("bitdrop_pool", default=4) == 9

    def test_returns_the_default_when_absent(self) -> None:
        assert parse("redis://h/0").int_option("bitdrop_pool", default=4) == 4

    @pytest.mark.parametrize("bad", ["0", "-1", "many", ""])
    def test_values_below_the_minimum_or_not_integers_are_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError, match="bitdrop_pool"):
            parse(f"redis://h/0?bitdrop_pool={bad}").int_option("bitdrop_pool", default=4)

    def test_the_minimum_can_be_changed(self) -> None:
        parsed = parse("redis://h/0?bitdrop_pool=0")

        assert parsed.int_option("bitdrop_pool", default=4, minimum=0) == 0


class TestStrOption:
    def test_returns_the_value_or_the_default(self) -> None:
        assert (
            parse("redis://h/0?bitdrop_name=abc").str_option("bitdrop_name", default="d") == "abc"
        )
        assert parse("redis://h/0").str_option("bitdrop_name", default="d") == "d"
