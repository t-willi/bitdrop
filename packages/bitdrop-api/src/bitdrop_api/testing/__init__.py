"""Conformance kits: reusable tests that every adapter of a contract must pass."""

from bitdrop_api.testing.chunk_queue_conformance import ChunkQueueConformance
from bitdrop_api.testing.chunk_store_conformance import MIB, ChunkStoreConformance, make_payload
from bitdrop_api.testing.destination_conformance import DestinationConformance, ReadBack
from bitdrop_api.testing.metadata_store_conformance import MetadataStoreConformance
from bitdrop_api.testing.sample_data import SAMPLE_SCHEMA, make_rows
from bitdrop_api.testing.source_conformance import SourceConformance

__all__ = [
    "MIB",
    "SAMPLE_SCHEMA",
    "ChunkQueueConformance",
    "ChunkStoreConformance",
    "DestinationConformance",
    "MetadataStoreConformance",
    "ReadBack",
    "SourceConformance",
    "make_payload",
    "make_rows",
]
