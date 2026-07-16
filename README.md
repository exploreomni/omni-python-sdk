# omni-python-sdk

Python SDK for the [Omni Analytics API](https://docs.omni.co/docs/API/), generated from the official OpenAPI spec. Covers the full public API surface (195 endpoints across queries, documents, models, connections, SCIM user/group management, schedules, AI, and more), with typed request/response models and both sync and async support.

## Installation

```bash
pip install omni-python-sdk
```

Requires Python 3.10+.

## Authentication

Create an API key in Omni under **Settings → API Keys**, then either export it:

```bash
export OMNI_API_KEY="your-api-key"
export OMNI_BASE_URL="https://myorg.omniapp.co"
```

(or put the same two lines in a `.env` file) and build a client:

```python
from omni_python_sdk.helpers import client_from_env

client = client_from_env()
```

Or construct one explicitly:

```python
from omni_python_sdk import AuthenticatedClient

client = AuthenticatedClient(base_url="https://myorg.omniapp.co", token="your-api-key")
```

## Running queries

The query endpoints return Apache Arrow data. The `helpers` module handles polling and decoding for you:

```python
from omni_python_sdk.helpers import client_from_env, run_query_blocking

client = client_from_env()

query = {
    "query": {
        "limit": 100,
        "sorts": [{"column_name": "order_items.created_at[date]"}],
        "table": "order_items",
        "fields": ["order_items.created_at[date]", "order_items.sale_price_sum"],
        "modelId": "your-model-id",
    }
}

table, fields = run_query_blocking(client, query)  # table is a pyarrow.Table
df = table.to_pandas()
```

Tip: copy a ready-made query body from any workbook via **View → Query Structure**.

> **Note:** always use these helpers for queries — don't call the generated
> `omni_python_sdk.api.query.query_run` / `query_wait` modules directly. The
> spec declares these responses as JSON, but the endpoints actually stream
> NDJSON with base64-encoded Arrow IPC data, which the generated response
> parsing can't handle. The helpers own that decoding (and job polling, and
> surfacing query errors).

## Calling any endpoint

Every API operation is a module under `omni_python_sdk.api.<tag>`, with four variants: `sync`, `sync_detailed`, `asyncio`, and `asyncio_detailed`.

```python
from omni_python_sdk.api.whoami import whoami
from omni_python_sdk.api.scim import scim_users_list
from omni_python_sdk.api.documents import documents_create

me = whoami.sync(client=client)

users = scim_users_list.sync(client=client, count="50")

response = documents_create.sync_detailed(client=client, body=...)
print(response.status_code, response.parsed)
```

Request/response models live in `omni_python_sdk.models` and convert to/from plain dicts with `.to_dict()` / `.from_dict()`.

Async is the same modules:

```python
result = await whoami.asyncio(client=client)
```

See [`examples/`](examples/) for end-to-end scripts (queries, user management, document migration, embed sessions, semantic-view generation).

## Migrating from 0.x

Version 1.0 is a full rewrite: the hand-written `OmniAPI` class is gone, replaced by the generated client above. The most common patterns map as follows:

| 0.x | 1.x |
|---|---|
| `OmniAPI()` | `client_from_env()` from `omni_python_sdk.helpers` |
| `api.run_query_blocking(body)` | `run_query_blocking(client, body)` from `omni_python_sdk.helpers` |
| `api.create_user(body)` etc. | `omni_python_sdk.api.scim.scim_users_create.sync(client=client, body=...)` etc. |
| `api.document_export(id)` | `omni_python_sdk.api.unstable.unstable_documents_export.sync(client=client, identifier=id)` |

## Regenerating the SDK

The client is generated from the vendored spec in `spec/openapi.json` using [openapi-python-client](https://github.com/openapi-generators/openapi-python-client) (the generator needs Python 3.11+, though the SDK itself runs on 3.10):

```bash
pip install openapi-python-client

scripts/generate.sh                                  # regenerate from the checked-in spec
scripts/generate.sh --url https://myorg.omniapp.co   # sync the spec from a live instance first
scripts/generate.sh --source ../omni/packages/bi-app/app/types/api/openapi/openapi.json
```

The pipeline preprocesses the spec (`scripts/preprocess_spec.py`), regenerates `omni_python_sdk/` (preserving the hand-written `helpers.py`), and CI fails if the checked-in generated code drifts from the checked-in spec. When the spec is synced (`--source`/`--url`), `spec/provenance.json` records where it came from — including the omni repo commit SHA — so every SDK version is traceable to an exact API state.

## Generated code policy

Everything in `omni_python_sdk/` **except `helpers.py`** is generated — don't edit it by hand; changes belong in the spec (upstream in the omni repo) or in the generation pipeline. Generated files are marked `linguist-generated` in `.gitattributes`, so GitHub collapses them in PR diffs.

**Reviewing a spec-sync PR:** review the `spec/openapi.json` diff and any hand-written changes; skip the generated diff. That's safe because CI's drift check proves the generated code is a pure function of the checked-in spec.

**Versioning:** the SDK follows its own semver, independent of the API's `info.version` — major for breaking surface changes, minor for new endpoints/fields (most spec syncs), patch for regeneration fixes. See [VERSIONING.md](VERSIONING.md) for how to classify a spec sync (including mechanical breaking-change detection with oasdiff), how to handle generator upgrades, and the release process. Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## Development

```bash
pip install -e '.[dev]'
pytest
```
