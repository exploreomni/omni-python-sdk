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
- Spec synced from omni repo commit `c3fe7934808a8086999643252e5c19d0917ed171`
  (see `spec/provenance.json`), which fixes the query endpoints' declared
  content types to NDJSON with typed stream-line models
  (exploreomni/omni#57144) and adds AI credit-control entity groups, model
  suggestions, and dashboard-removal endpoints (128 paths / 201 operations).
