# bitdrop

Very fast bulk data transfer between databases, using an elastic in-RAM staging tier (Redis) so extraction and loading
run in parallel at the speed of the source and the destination — and the RAM is only paid for while the job runs.

**Status:** ideation. See [docs/ideation.md](docs/ideation.md).
