# Using custom settings

Most callers of this API are not humans: they are instances of the
[dots-vue](https://github.com/dots-suite/dots-vue) front-end, which builds its requests from a JSON
settings file **per collection**. Those files live in a settings repository — for example
[dots-vue-demo-settings](https://github.com/dots-suite/dots-vue-demo-settings) — and two of their keys
concern search directly.

Nothing here affects indexing. The only settings key the CLI reads is `excludeCollectionIds`; see
[Indexing](indexing.md#excluded-collections).

## `customRoutes` — whether the collection has a search page at all

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

## `searchConfig` — which facets are offered

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
| `key` | A **canonical metadata key** — the same vocabulary the API publishes (`dublinCore.creator`, `extensions.author`), plus the special value `collections`. |
| `enabled: false` | The key is collected and sent as **`excludeFacets`** (or **`excludeTemporalFacets`** for the temporal array). The facet is neither aggregated by Elasticsearch nor displayed. |
| `label` | Front-end only: overrides the displayed name. |
| `order` | Front-end only: display order. |

!!! important "The logic is exclusion-based, not inclusion-based"
    A facet **absent** from the configuration is still aggregated and still displayed. Listing a facet
    with `enabled: true` documents an intent but changes nothing by itself; only `enabled: false`
    has an effect. A partial configuration therefore never hides the facets it forgets to mention.

## What the front-end actually sends

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
