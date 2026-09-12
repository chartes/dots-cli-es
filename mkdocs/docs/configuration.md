# Configuration

All runtime settings live in three YAML files shipped with the package:

```
dots_es/config/
├── local.yml
├── staging.yml
└── prod.yml
```

One of them is selected by the global `--config` option of the CLI
(`--config [local|staging|prod]`, **default `staging`**), and by `--config` / the `SERVER_ENV_CONFIG`
environment variable for the API.

## Keys

### `source:` — where the corpus comes from

| Key | Controls |
|---|---|
| `DTS_URL` | The DoTS/DTS endpoint. Passed to `ThunderDots(endpoint_dts=…)`, used to resolve the root collection, and used by the API to build the `dts_url` of each hit. |
| `TARGET_COLLECTION` | Identifier of the collection to crawl. **Case-sensitive** — it must match the DTS identifier exactly (`ENCPOS`, not `encpos`). **Empty** means "start from the DTS root collection", which is resolved at runtime. |
| `CUSTOM_SETTINGS_PATH` | Directory of front-end `*.conf.json` settings files. Every `excludeCollectionIds` entry found there is added to the exclusion set. Environment-interpolated. |
| `ADDITIONAL_EXCLUDED_COLLECTIONS` | List of collection ids to skip, merged with the ones derived from `CUSTOM_SETTINGS_PATH`. **Case-insensitive**, unlike `TARGET_COLLECTION`: both the list and the candidate identifier are lowercased before comparison, so `ENCPOS` and `encpos` are equivalent here. |

### `config:` — Elasticsearch and the API

| Key | Controls |
|---|---|
| `ELASTICSEARCH_URL` | ES endpoint used by both the CLI and the API. |
| `DOCUMENT_INDEX` | Index holding resources **and** passages. Default `dots_document`. |
| `COLLECTION_INDEX` | Index holding collections. Default `dots_collection`. |
| `SEARCH_RESULT_PER_PAGE` | Default `page[size]` of the search API. Default `200`. |

### Differences between the three files

| | `local` | `staging` | `prod` |
|---|---|---|---|
| `DTS_URL` | `http://localhost:8080/api/dts` — DoTS installed locally, on its default port<br>or any reachable DTS endpoint, e.g. `https://dev.chartes.psl.eu/dots/api/dts` | `https://dev.chartes.psl.eu/dots/api/dts` | `https://dots.chartes.psl.eu/demo/api/dts/collection` |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | `http://elastic:${ES_PASSWORD}@127.0.0.1:9200` | idem staging |

Only the two endpoints differ. `TARGET_COLLECTION` is `""` in all three files — every environment
therefore crawls from the **DTS root collection** — and `ADDITIONAL_EXCLUDED_COLLECTIONS` is empty
everywhere, so nothing is skipped unless `CUSTOM_SETTINGS_PATH` contributes exclusions.

## Environment variables

| Variable | Used by | Effect |
|---|---|---|
| `ES_PASSWORD` | CLI + API | Interpolated into `ELASTICSEARCH_URL` — only meaningful for `staging` and `prod`. |
| `CUSTOM_SETTINGS_PATH` | CLI | Directory scanned for `*.conf.json` front-end settings. If unset or not a directory, no error: the exclusion set is simply empty. |
| `SERVER_ENV_CONFIG` | API only | Overrides the `--config` argument. Intended for server environments. |

!!! warning "Identifiers are case-sensitive on the DoTS side"
    `TARGET_COLLECTION` — like `--collections` — is sent to the endpoint verbatim, and DoTS matches
    identifiers exactly: `ENCPOS` resolves, `encpos` does not. A wrong case produces an **empty
    crawl, not an error**. Check the identifier against the endpoint first:

    ```bash
    curl "https://dots.chartes.psl.eu/demo/api/dts/collection?id=ENCPOS"
    ```

    The exclusion list is the exception: it is compared in lowercase on both sides, so its case does
    not matter.

Typical invocation with security enabled:

```bash
ES_PASSWORD=your_password dots-es-cli --config=prod index
```

## How the files are loaded

`load_config(alias)` resolves `dots_es/config/{alias}.yml` through `importlib.resources`, so it works
from an installed wheel as well as from a checkout. It then:

1. replaces every `None` with an empty string;
2. expands environment variables in **every** string value;
3. **flattens** `source:` and `config:` into a single dictionary — `app.config["DTS_URL"]` and
   `app.config["DOCUMENT_INDEX"]` sit side by side;
4. coerces `ADDITIONAL_EXCLUDED_COLLECTIONS` into a lowercase set.

!!! warning "Operational caveats"
    - **A non-editable install freezes these files.** Because they are read from the *installed*
      package, `pip install .` means the CLI uses the copy in `site-packages`, not the one in your
      clone. Editing `dots_es/config/local.yml` then changes nothing until you reinstall. Use
      `pip install -e .` while you are still adjusting the configuration.
    - **An unset variable is left as literal text.** `${ES_PASSWORD}` stays `${ES_PASSWORD}` in the
      URL rather than becoming empty, which surfaces as a confusing connection error. Check that the
      variable is exported before blaming Elasticsearch.
    - **The resolved configuration is printed on stdout at every run**, including the password
      embedded in `ELASTICSEARCH_URL`. Keep that in mind for CI logs and shared terminals.
    - Because the two blocks are flattened into one dictionary, a key present in both `source:` and
      `config:` would be silently resolved in favour of `config:`.

## Keys that are *not* configurable

Two values are read from the config dictionary but declared in none of the YAML files, so they always
fall back to their literal defaults:

| Key | Default |
|---|---|
| `MAX_CONCURRENT_REQUESTS` | `5` |
| `RESOURCE_WORKERS` | `5` |

Adding them to the `config:` block is enough to make them effective.

!!! note "No more `.env`"
    Earlier versions used `.env` files with `python-dotenv`. They were dropped in favour of these
    YAML files; only the environment variables listed above remain.
