# Search API

```bash
dots-api [--config local|staging|prod]
```

The Flask application listens on **port 5003**, on `localhost`, with debug enabled. On servers it is
served through uWSGI as `flask_app:flask_app`, and the `SERVER_ENV_CONFIG` environment variable
overrides `--config`.

Smoke test:

```
http://localhost:5003/api/1.0/search?query=*&index=dots_document
```

## The endpoint

A single route is exposed:

```
GET /api/1.0/search
```

Responses are `application/json; charset=utf-8` with `Access-Control-Allow-Origin: *`. Errors return
**HTTP 400** with the exception text as the body.

## Two modes

The `no-highlight` parameter is a **mode switch**:

=== "Full-text mode (default)"

    `no-highlight` absent. Queries fragments (`type.keyword == "fragment"`), collapses results by
    `resource_id` with `inner_hits` named `fragments`, and highlights `content` with the **`fvh`**
    highlighter (`<mark>` tags, `fragment_size: 80`, `number_of_fragments: 100`,
    `fragment_offset: 25`, `no_match_size: 50`).

    Response: `{buckets, facets, bucket_count, total_count, page, page_size, highlight_patterns, temporal}`.

=== "Notice mode"

    `no-highlight` present. Returns resource records rather than highlighted fragments.

    Response: `{data, total_count, facets, highlight_patterns, temporal}`, where each `data` item is a
    `resource_id` plus flattened `resource_metadata` and unflattened `temporal`.

!!! warning "Presence, not value"
    The switch tests whether the parameter is a string, so **`no-highlight=false` also enables notice
    mode**. Omit the parameter entirely to stay in full-text mode.

Both responses also carry `collection_indexed` and a `duration` in seconds.

## Query parameters

| Parameter | Default | Effect |
|---|---|---|
| `index` | `DOCUMENT_INDEX` | Target Elasticsearch index. |
| `query` | `match_all` | Supports exact phrases, `AND`/`OR`/`NOT`, `*` and `?` wildcards, and `field:value` with aliases. Default operator is `AND`, wildcards are analyzed. The available field aliases differ between the two modes. |
| `no-highlight` | absent | Mode switch, see above. |
| `collectionId` | none | Scopes the search to a collection subtree; also drives `collection_indexed` in the response. |
| `collections` | none | `[a,b]` list of collection keys to **remove** from the returned collection facets. |
| `facets` | none | JSON object `{canonical_key: [values]}` of selected facet values. `collections` uses OR logic; every other facet uses AND. |
| `excludeFacets` <sup>*</sup> | none | Comma-separated canonical keys not to compute or return. `collections` is a valid value. |
| `excludeTemporalFacets` <sup>*</sup> | none | Same, for temporal range facets. |
| `range[<field>]` | none | Repeated-key syntax, e.g. `range[dublinCore.created]=gte:1200,lte:1300`. |
| `filters` | none | `field:value1\|value2,field2:value3` — one clause per comma, values within a field joined with `OR`. Each clause becomes a `query_string` restricted to that field. |
| `page[number]` | `1` | Offset pagination. |
| `page[size]` | `SEARCH_RESULT_PER_PAGE` (200) | **Minimum 25**, no maximum. |
| `sort` | `dublinCore.created` ascending, then `_score` descending | Comma-separated criteria; a `-` prefix means descending. Missing values sort last. |

<sup>*</sup> These two parameters exist mainly for the
[dots-vue](https://github.com/dots-suite/dots-vue) front-end and the per-collection settings it
reads. You rarely build them by hand: see [Collection settings](#collection-settings) below, and the
example repository [dots-vue-demo-settings](https://github.com/dots-suite/dots-vue-demo-settings).

!!! tip "`filters` versus `facets`"
    Both narrow the result set, but they are not interchangeable. `facets` takes a JSON object keyed
    by **canonical** metadata keys and is what the front-end sends when a user ticks a facet value;
    `filters` is a compact string form resolved directly against **Elasticsearch field names**, which
    makes it handy for hand-written queries and debugging.

    ```
    filters=resource_metadata.dublincore.creator:Molière|Racine
    ```

## Facets

Facet aggregations are generated from the [search field registry](search-fields.md): one `terms`
aggregation per non-range `KEYWORD` field declared with `facet=True`, each with a `cardinality`
sub-aggregation on `resource_id` so that **counts are per resource, not per fragment**.

Buckets are computed under the field `id` and republished to clients under the canonical `key`
(`dct:creator` → `dublinCore.creator`).

## Sorting

- Temporal fields sort on their `temporal.{range_start}` bound.
- Text, keyword and URL fields sort on the `.sort` sub-field produced by the `sortable` normalizer —
  never on `.keyword`. That normalizer strips leading punctuation, lowercases and folds accents, which
  is what makes `« Tragédie »` sort next to `Tragédie`.

## Collection settings

Most callers of this API are not humans: they are instances of the
[dots-vue](https://github.com/dots-suite/dots-vue) front-end, which builds its requests from a JSON
settings file **per collection**. Those files live in a settings repository — for example
[dots-vue-demo-settings](https://github.com/dots-suite/dots-vue-demo-settings) — and two of their keys
concern search directly.

Nothing here affects indexing. The only settings key the CLI reads is `excludeCollectionIds`; see
[Indexing](indexing.md#excluded-collections).

### `customRoutes` — whether the collection has a search page at all

```json
"customRoutes": [
  {
    "name": "Search",
    "path": "search",
    "compName": "SearchPage"
  }
]
```

This is pure routing: it decides that `/<collectionId>/search` exists and which component answers
there. Without this entry the URL redirects to the collection home, and the collection simply has no
search page — so no request ever reaches this API for it.

`SearchPage` is case-sensitive.

### `searchConfig` — which facets are offered

```json
"searchConfig": {
  "facets": [
    {
      "key": "dublinCore.creator",
      "label": "Auteurs",
      "enabled": true,
      "order": 3
    },
    {
      "key": "dublinCore.publisher",
      "enabled": false
    }
  ],
  "temporalFacets": [
    {
      "key": "dublinCore.created",
      "label": "Promotion (période)",
      "enabled": true,
      "order": 1
    },
    {
      "key": "dublinCore.issued",
      "enabled": false
    }
  ]
}
```

| Field | Effect |
|---|---|
| `key` | A **canonical metadata key** — the same vocabulary this API publishes (`dublinCore.creator`, `extensions.author`), plus the special value `collections`. |
| `enabled: false` | The key is collected and sent as **`excludeFacets`** (or **`excludeTemporalFacets`** for the temporal array). The facet is neither aggregated by Elasticsearch nor displayed. |
| `label` | Front-end only: overrides the displayed name. |
| `order` | Front-end only: display order. |

!!! important "The logic is exclusion-based, not inclusion-based"
    A facet **absent** from the configuration is still aggregated and still displayed. Listing a facet
    with `enabled: true` documents an intent but changes nothing by itself; only `enabled: false`
    has an effect. A partial configuration therefore never hides the facets it forgets to mention.

### What the front-end actually sends

Taking the `ENCPOS` settings as an example — `collections` and `dublinCore.creator` enabled, the rest
disabled — the first request of the search page is:

```
GET /api/1.0/search
  ?query=&page[number]=1&page[size]=25
  &collectionId=ENCPOS
  &excludeFacets=dublinCore.contributor,dublinCore.publisher,dublinCore.language,extensions.author,…
  &excludeTemporalFacets=dublinCore.issued,extensions.dateCreated,…
```

The excluded keys appear in the order of the arrays in the file. Because `collections` is not
excluded here, the collections aggregation is built and rendered — under the label the settings give
it.

!!! warning "Arrays are replaced, not merged"
    A collection's settings are merged over the project-wide ones, but **arrays are replaced
    wholesale**. A collection that redefines `facets` supersedes the root `facets` entirely, while
    still inheriting the root `temporalFacets` if it does not redefine them. Copy the whole array
    when you override one.
