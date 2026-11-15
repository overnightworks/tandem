# tandem

## Commands

- `scripts/run_static_checks.sh`: every static check of CI's `checks` job, from any directory; it stops at the first failure.
- `uvx --from "git+https://github.com/overnightworks/quality@<sha>" quality check`, with the sha `.github/workflows/ci.yml` pins: the quality gate's local check.
- `scripts/run_redis_tests.sh <pytest argument>...`: the tests against throwaway Redis containers, for example `scripts/run_redis_tests.sh tests/test_wake_basic.py`; it waits for a low load and removes its containers and an image it pulled when it ends. CI runs the suite through it. Run only the modules that prove a change, with `PYTEST_XDIST_AUTO_NUM_WORKERS=4`.
- `scripts/regenerate_build_constraints.sh`: regenerates `.github/requirements-build.txt` after a change to the setuptools floor in `[build-system]` of `pyproject.toml`.
- `gh workflow run release.yml -R overnightworks/tandem -f version=X.Y.Z`: an agent dispatches the release once the review and CI of the release pull request are green on `main`; `main` must carry X.Y.Z in `tandem.__version__`.

## Rules

- `tandem.__version__` is the one version. It stays a plain string literal and is raised alone, in the pull request before the release.
- A tag is never moved or re-created; a broken release gets the next patch tag. A gate refusal leaves no tag, so the release is dispatched again once its cause is gone; after a failed `tag` job, re-run the failed jobs, never all jobs.
- Redis is the one runtime dependency; fakeredis, pytest and the other test tools are dev-only, and `.importlinter` keeps them out of `tandem`.
- Every name tandem gives in Redis is built in `tandem.keys`; no other module writes a key or channel literal.
- Tests run every cell against a real Redis through `scripts/run_redis_tests.sh`; outages go through `tests/support/proxy.py`; waits are barriers and bounded waits, never sleeps. Each test uses the `key_prefix` fixture, so tests share one server in parallel. Branch coverage stays 100 %.
- The published channel name and the `Resubscribed` ordering are a contract between pods of different releases: a change to `<prefix>:tandem:v1:wake` raises the version segment.
- A name is public when `tandem/__init__.py`'s `__all__` lists it. A pull request that changes a public name states its **Breaking** line and the hosts' migration or "No migration".

## Owners

- An agent dispatches `release.yml` once review and CI are green on `main`.
- The Redis the fleet's apps share, its ACLs and its restarts are the platform's.

## Documents

- `README.md`: read when you change what a host calls, configures or must do.
- `docs/ARCHITECTURE.md`: read when you change which module owns what, or a boundary.
- `docs/VISION.md`: read when you judge whether a capability belongs in tandem.
