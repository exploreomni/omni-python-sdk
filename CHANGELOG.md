# Changelog

## 1.0.0 (unreleased)

Full rewrite: the SDK is now generated from the official Omni OpenAPI spec.

- **Breaking:** the hand-written `OmniAPI` class is removed. Queries move to
  `omni_python_sdk.helpers` (`client_from_env`, `run_query_blocking`,
  `wait_query_blocking`); all other operations are generated endpoint modules
  under `omni_python_sdk.api.<tag>` — see the README migration table.
- **Breaking:** errors now raise (`httpx` exceptions / typed responses)
  instead of printing and returning `None`.
- **Breaking:** Python 3.10+ required (was 3.9+).
- Coverage grows from ~30 hand-written endpoints to all 195 operations in the
  spec (queries, documents, models, connections, SCIM, schedules, AI, embed,
  and more), with typed models and sync + async variants.
- Packaging modernized to `pyproject.toml`; fixes the incorrect `dotenv`
  dependency (now `python-dotenv`).
- Spec synced from omni repo commit `7805fc5e5dcc6bcd3fbc39885b5f675fa8470195`
  (see `spec/provenance.json`).
