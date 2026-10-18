# One image for every role of the queue workflow (planner, source, destination): the role is the
# command, e.g. `bitdrop queue source /job.toml`. It holds the command line tool and the adapters
# for Redis and PostgreSQL, and no display toolkit, so it stays small.
#
#     docker build -f docker/Dockerfile -t bitdrop:dev .
#
# Add an adapter by adding its package to the list below.

FROM python:3.14-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv
WORKDIR /src
COPY packages ./packages
# --no-sources: the packages name each other as `workspace = true`, which would install them editable,
# as links to /src, a folder the final image does not have.
RUN uv venv /opt/venv \
 && uv pip install --python /opt/venv/bin/python --no-cache --no-sources \
    ./packages/bitdrop-api \
    ./packages/bitdrop-core \
    ./packages/bitdrop-cli \
    ./packages/bitdrop-store-redis \
    ./packages/bitdrop-queue-redis \
    ./packages/bitdrop-marker-redis \
    ./packages/bitdrop-connector-postgres

FROM python:3.14-slim
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
# Not root: a container that only reads, stages, and loads needs no privileges.
USER 10001
ENTRYPOINT ["bitdrop"]
CMD ["--help"]
