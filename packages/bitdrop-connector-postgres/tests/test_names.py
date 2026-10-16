"""Table names go into SQL text, so they must be validated strictly and quoted always."""

import pytest

from bitdrop_connector_postgres._names import parse_table_name, quote_ident


class TestParseTableName:
    def test_a_plain_name(self) -> None:
        name = parse_table_name("orders")

        assert (name.schema, name.name) == (None, "orders")
        assert name.sql() == '"orders"'
        assert str(name) == "orders"

    def test_a_schema_qualified_name(self) -> None:
        name = parse_table_name("sales.orders")

        assert (name.schema, name.name) == ("sales", "orders")
        assert name.sql() == '"sales"."orders"'
        assert str(name) == "sales.orders"

    def test_case_is_preserved_by_quoting(self) -> None:
        assert parse_table_name("Orders").sql() == '"Orders"'

    def test_underscores_digits_and_dollars_are_allowed_after_the_first_character(self) -> None:
        assert parse_table_name("_t1$x").name == "_t1$x"

    @pytest.mark.parametrize(
        "text",
        [
            "",
            "a.b.c",
            "1abc",
            "has space",
            'quote"d',
            "semi;colon",
            "a-b",
            "x" * 64,
            ".a",
            "a.",
            "orders; DROP TABLE users",
            "orders\n",
            "ünïcode",
        ],
    )
    def test_anything_else_is_rejected(self, text: str) -> None:
        with pytest.raises(ValueError, match="table name"):
            parse_table_name(text)

    def test_the_longest_legal_identifier_is_accepted(self) -> None:
        assert parse_table_name("x" * 63).name == "x" * 63


class TestQuoteIdent:
    def test_wraps_in_double_quotes(self) -> None:
        assert quote_ident("id") == '"id"'

    def test_doubles_embedded_quotes(self) -> None:
        """Column names come from the catalog and may contain anything."""
        assert quote_ident('we"ird') == '"we""ird"'

    def test_spaces_and_keywords_are_safe_once_quoted(self) -> None:
        assert quote_ident("select from") == '"select from"'

    @pytest.mark.parametrize("bad", ["", "nul\x00byte"])
    def test_identifiers_postgres_cannot_hold_are_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError, match="identifier"):
            quote_ident(bad)
