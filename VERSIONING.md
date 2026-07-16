# Versioning

This SDK follows [semantic versioning](https://semver.org/). The SDK version is
**independent of the API's `info.version`** in the OpenAPI spec and of Omni app
releases — it describes the SDK's own surface: the generated client, the
hand-written `helpers.py`, and the package's runtime requirements.

The version lives in one place: `pyproject.toml`.

## What each bump means

| Bump | When | Examples |
|---|---|---|
| **Major** | A change a working program could break on | Endpoint or model removed/renamed; parameter or field type changed; required parameter added; `helpers.py` signature changed; Python version floor raised; generator upgrade that reshapes generated signatures |
| **Minor** | Purely additive surface | New endpoints or tags; new models; new optional fields or parameters; new helper functions |
| **Patch** | No surface change | Regeneration fixes; docs; dependency pin adjustments; internal generation-pipeline changes |

Most spec syncs are **minor**. The Omni API's own CI runs breaking-change
detection (oasdiff) before spec changes merge, so removals should be rare —
but the SDK sync is where they become a package consumer's problem, so
classify each sync explicitly.

## Classifying a spec sync

1. Sync the spec: `scripts/generate.sh --source ../omni/.../openapi.json`
2. Look at the diff summary: `git diff --stat spec/openapi.json` and the
   generated diff (`git diff --stat omni_python_sdk/`). Deleted or renamed
   modules under `omni_python_sdk/api/` or `omni_python_sdk/models/` are a
   strong breaking signal; only-added files suggest minor.
3. For a mechanical verdict, run [oasdiff](https://github.com/oasdiff/oasdiff)
   against the previous spec:

   ```bash
   git show HEAD:spec/openapi.json > /tmp/openapi.old.json
   oasdiff breaking /tmp/openapi.old.json spec/openapi.json
   ```

   Any reported breaking change → major bump (or push back on the API change
   upstream before shipping it).

## Generator upgrades

The `openapi-python-client` version is pinned in `pyproject.toml` (dev extras).
Upgrading it can rewrite every generated file with **no API change** — and can
also change generated method signatures, which is breaking for SDK users even
though the API didn't move.

- Land generator upgrades as their own PR, clearly labeled, never mixed with a
  spec sync.
- Diff the generated output before/after: if signatures or model shapes
  changed, it's a major bump; if only formatting/internals changed, patch.

## Release process

1. Decide the bump (above) and update `version` in `pyproject.toml`.
2. Add a `CHANGELOG.md` entry: what changed in API surface terms (endpoints
   added/removed, helpers changed), and the omni commit from
   `spec/provenance.json` the spec was synced from.
3. Merge to `main`, then create a GitHub release tagged `vX.Y.Z`.
4. The `python-publish.yml` workflow builds and publishes to PyPI via trusted
   publishing on release publish — no manual upload.

Every release is traceable: the git tag pins the spec (`spec/openapi.json`)
and `spec/provenance.json` pins the omni repo commit that spec came from.
