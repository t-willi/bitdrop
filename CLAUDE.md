# bitdrop: instructions for AI agents

**Start by reading `docs/agent-log.md`.** It records what exists, what was decided and why, what is not done, and the
traps that cost time. Update it before you finish (section 9 of that file).

## Orientation

bitdrop moves database tables in parallel through an elastic in-RAM staging tier, with crash-safe, duplicate-free
loading. Python 3.14, a uv workspace (`packages/`), Apache Arrow as the one data model. Design rules are in
`docs/principles.md`; commands, layout, and how to extend it are in `docs/development.md`.

```bash
make sync        # install
make test-fast   # no Docker, about 5 seconds
make check       # lint + strict types + architecture rules + every test (some need Docker)
```

## Rules that always apply

- Test first: see the test fail for the right reason, then implement; then break the code on purpose
  (`scripts/mutation_check.sh`) to prove the tests notice.
- Every component sits behind a contract and is selected by URL; the core never names a backend (P1). Never silently
  alter data: store a value exactly or refuse it loudly (P2).
- Clean, documented code; one workspace package per component; steps documented in `docs/`.
- Run `make check` before committing. Committing without asking is allowed once it passes.
- Commit small and often, so any step can be reverted alone: one logical change per commit, each passing `make check`
  on its own. A test goes in with the code that makes it pass; a refactor, a tooling fix, a doc update, and a bug found
  on the way are separate commits. Never batch a session's work into one big commit.
- Tell the user when a milestone test succeeds. Do not touch Docker containers the tests did not start.
- Keep secrets out of messages and logs.
