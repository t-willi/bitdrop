"""The job file: a TOML description of one transfer, and how it becomes a ``TransferConfig``.

.. code-block:: toml

    [job]
    id = "orders-1"                 # names the job; running the same id again resumes it

    [source]
    url = "${SOURCE_URL}"           # scheme://... picks the connector; ${NAME} = environment
    table = "public.orders"
    split_column = "id"             # optional hint for sources that split by a key

    [destination]
    url = "${DESTINATION_URL}"
    table = "orders"

    [staging]                       # where chunks wait in RAM
    url = "memory://?max_bytes=4000000000"

    [metadata]                      # chunk states, leases, retries; use a persistent one to resume
    url = "scheme:///path/to/state"

    [tuning]                        # all optional; leave out what you do not need
    extractors = 4
    loaders = 4
    chunk_mib = 64

A misspelled key or section is an error, never a silent default. Passwords belong in the environment
(``${NAME}``), and no error message ever repeats a value from the file.
"""

import os
import re
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from bitdrop_api.errors import BitdropError, InvalidKey
from bitdrop_api.keys import validate_key
from bitdrop_api.urls import redact_url
from bitdrop_core.transfer import MAX_JOB_ID_LENGTH, MIB, TransferConfig

__all__ = ["JobFile", "JobFileError", "load_job"]

_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class JobFileError(BitdropError):
    """The job file cannot be used. The message lists every problem found."""


class _Section(BaseModel):
    # hide_input_in_errors: a validation error must never repeat what was in the file (it may be a
    # URL with a password).
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


def _expand(value: object, info: ValidationInfo) -> object:
    """Replace ``${NAME}`` in a URL by the environment variable ``NAME``."""
    if not isinstance(value, str):
        return value  # the type check reports it, without echoing it
    environ: Mapping[str, str] = (info.context or {}).get("environ", {})

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in environ:
            raise ValueError(f"environment variable {name} is not set")
        return environ[name]

    return _VARIABLE.sub(replace, value)


class _UrlSection(_Section):
    url: str
    """``scheme://...``; ``${NAME}`` is replaced by the environment variable ``NAME``."""

    @field_validator("url", mode="before")
    @classmethod
    def _expand_variables(cls, value: object, info: ValidationInfo) -> object:
        return _expand(value, info)


class _QueuesSection(_Section):
    work: str
    """The queue of chunk ids that source containers read."""

    files: str
    """The queue of staged chunks that destination containers load."""

    @field_validator("work", "files", mode="before")
    @classmethod
    def _expand_variables(cls, value: object, info: ValidationInfo) -> object:
        return _expand(value, info)


class _JobSection(_Section):
    id: str

    @field_validator("id")
    @classmethod
    def _valid_id(cls, value: str) -> str:
        try:
            validate_key(value)
        except InvalidKey as error:
            raise ValueError(str(error)) from None
        if len(value) > MAX_JOB_ID_LENGTH:
            raise ValueError(f"must be at most {MAX_JOB_ID_LENGTH} characters")
        return value


class _SourceSection(_UrlSection):
    table: str
    split_column: str | None = None


class _DestinationSection(_UrlSection):
    table: str


class _TuningSection(_Section):
    extractors: int | None = Field(default=None, ge=1)
    loaders: int | None = Field(default=None, ge=1)
    chunk_mib: float | None = Field(default=None, gt=0)
    lease_seconds: float | None = Field(default=None, gt=0)
    max_attempts: int | None = Field(default=None, ge=1)
    chunk_timeout: float | None = Field(default=None, gt=0)
    backpressure_timeout: float | None = Field(default=None, gt=0)


class JobFile(_Section):
    """A validated job file."""

    job: _JobSection
    source: _SourceSection
    destination: _DestinationSection
    staging: _UrlSection
    metadata: _UrlSection | None = None
    """Chunk states for the master-based runner (``bitdrop run``)."""

    queues: _QueuesSection | None = None
    """The two queues of the queue workflow (``bitdrop queue ...``)."""

    markers: _UrlSection | None = None
    """Where the planner records its settings, for the queue workflow."""

    tuning: _TuningSection = Field(default_factory=_TuningSection)

    @model_validator(mode="after")
    def _one_workflow(self) -> JobFile:
        if self.metadata is None and self.queues is None:
            raise ValueError("the job needs a [metadata] or [queues] section")
        return self

    def require_metadata(self) -> _UrlSection:
        """The metadata store of the master-based runner.

        Raises:
            JobFileError: the job file has no ``[metadata]`` section.
        """
        if self.metadata is None:
            raise JobFileError("this command needs a [metadata] section in the job file")
        return self.metadata

    def require_queues(self) -> _QueuesSection:
        """The queues of the queue workflow.

        Raises:
            JobFileError: the job file has no ``[queues]`` section.
        """
        if self.queues is None:
            raise JobFileError("this command needs a [queues] section in the job file")
        return self.queues

    def require_markers(self) -> _UrlSection:
        """The marker store of the queue workflow.

        Raises:
            JobFileError: the job file has no ``[markers]`` section.
        """
        if self.markers is None:
            raise JobFileError("this command needs a [markers] section in the job file")
        return self.markers

    def transfer_config(
        self,
        *,
        job_id: str | None = None,
        extractors: int | None = None,
        loaders: int | None = None,
    ) -> TransferConfig:
        """Build the runner's configuration; the arguments override what the file says."""
        tuning = self.tuning
        options: dict[str, Any] = {
            "extractors": extractors or tuning.extractors,
            "loaders": loaders or tuning.loaders,
            "target_chunk_bytes": None if tuning.chunk_mib is None else int(tuning.chunk_mib * MIB),
            "lease_seconds": tuning.lease_seconds,
            "max_attempts": tuning.max_attempts,
            "chunk_timeout": tuning.chunk_timeout,
            "backpressure_timeout": tuning.backpressure_timeout,
        }
        return TransferConfig(
            job_id=job_id or self.job.id,
            source_table=self.source.table,
            destination_table=self.destination.table,
            split_column=self.source.split_column,
            **{name: value for name, value in options.items() if value is not None},
        )

    def endpoints(self) -> dict[str, str]:
        """Return the URLs the job uses with passwords hidden, safe to print."""
        urls = {
            "source": self.source.url,
            "destination": self.destination.url,
            "staging": self.staging.url,
        }
        if self.metadata is not None:
            urls["metadata"] = self.metadata.url
        if self.queues is not None:
            urls["work"] = self.queues.work
            urls["files"] = self.queues.files
        if self.markers is not None:
            urls["markers"] = self.markers.url
        return {role: redact_url(url) for role, url in urls.items()}


def load_job(path: Path, *, environ: Mapping[str, str] | None = None) -> JobFile:
    """Read and validate the job file at ``path``.

    ``environ`` supplies the variables for ``${NAME}`` (default: the process environment).

    Raises:
        JobFileError: the file is missing, is not TOML, or has problems; all are listed.
    """
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise JobFileError(f"cannot read the job file {path}: {error.strerror}") from None
    except tomllib.TOMLDecodeError as error:
        raise JobFileError(f"{path}: not valid TOML: {error}") from None

    try:
        return JobFile.model_validate(
            raw, context={"environ": os.environ if environ is None else environ}
        )
    except ValidationError as error:
        problems = [
            f"{path}: {'.'.join(str(part) for part in problem['loc'])}: {problem['msg']}"
            for problem in error.errors(include_input=False, include_url=False)
        ]
        raise JobFileError("\n".join(problems)) from None
