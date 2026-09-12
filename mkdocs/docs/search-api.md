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
reads. You rarely build them by hand: see [Using custom settings](custom-settings.md), and the
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
