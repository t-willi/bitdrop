"""Table and column names, which end up inside SQL text.

Values can be passed as query parameters; identifiers cannot. So table names are validated against
a strict grammar *and* always quoted, and column names (arbitrary strings from the catalog) are
always quoted with embedded quotes doubled.
"""

import re
from dataclasses import dataclass

# Letters, digits, "_" and "$"; not starting with a digit; Postgres truncates beyond 63 bytes.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]{0,62}")


@dataclass(frozen=True, slots=True)
class TableName:
    """A validated ``table`` or ``schema.table`` name."""

    schema: str | None
    name: str

    def sql(self) -> str:
        """Return the name quoted for use in SQL text."""
        quoted = quote_ident(self.name)
        return quoted if self.schema is None else f"{quote_ident(self.schema)}.{quoted}"

    def __str__(self) -> str:
        """Return the name as the user wrote it."""
        return self.name if self.schema is None else f"{self.schema}.{self.name}"


def parse_table_name(text: str) -> TableName:
    """Parse ``table`` or ``schema.table``.

    Names are case-sensitive (they are quoted), so ``Orders`` is not ``orders``.

    Raises:
        ValueError: the text is not a plain one- or two-part name.
    """
    parts = text.split(".")
    if len(parts) > 2 or any(_IDENTIFIER.fullmatch(part) is None for part in parts):
        raise ValueError(
            f"invalid table name {text!r}: use 'table' or 'schema.table', made of letters, "
            "digits, '_' and '$', not starting with a digit"
        )
    return TableName(schema=parts[0] if len(parts) == 2 else None, name=parts[-1])


def quote_ident(identifier: str) -> str:
    """Quote ``identifier`` for SQL text, doubling embedded double quotes.

    Raises:
        ValueError: the identifier is empty or contains a NUL character, which Postgres rejects.
    """
    if not identifier or "\x00" in identifier:
        raise ValueError(f"invalid identifier {identifier!r}")
    return '"' + identifier.replace('"', '""') + '"'
