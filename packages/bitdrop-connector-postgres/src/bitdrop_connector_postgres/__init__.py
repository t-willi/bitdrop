"""PostgreSQL Source and Destination connectors (Arrow-native, via ADBC).

Select them with ``postgresql://user:password@host:port/database``. Option ``bitdrop_pool_size``
(default 4) bounds the number of concurrent connections; every other query option goes to libpq.
"""

from bitdrop_connector_postgres.destination import PostgresDestination, create_destination
from bitdrop_connector_postgres.source import PostgresSource, create_source

__all__ = ["PostgresDestination", "PostgresSource", "create_destination", "create_source"]
