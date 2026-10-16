"""Repository-wide pytest configuration."""

import pytest

# The conformance kit lives in an importable package, not in a test module. Registering it here
# (before it is imported) makes pytest show detailed assertion diffs for its checks too.
pytest.register_assert_rewrite("bitdrop_api.testing")
