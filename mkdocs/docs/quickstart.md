# Quick start

This page walks through a full local cycle: configure, index, inspect, search.

It assumes Elasticsearch is running with the ICU plugin and that the package is installed — see
[Installation](installation.md).

## 1. Activate the environment

```bash
cd path/to/dots-cli-es
source your_venv_name/bin/activate
```

## 2. Pick a configuration

The CLI reads one of three YAML files through the global `--config` option
(`local`, `staging` or `prod`; **default `staging`**). For a first run, use `local`:

```yaml title="dots_es/config/local.yml (excerpt)"
source:
  DTS_URL: "https://dev.chartes.psl.eu/dots/api/dts"
  TARGET_COLLECTION: "cartulaires"
config:
  ELASTICSEARCH_URL: "http://localhost:9200"
  DOCUMENT_INDEX: "dots_document"
  COLLECTION_INDEX: "dots_collection"
```

`TARGET_COLLECTION` is the root of the crawl. Keep it small for a first run — see
[Configuration](configuration.md) for every key.

## 3. Create the indexes

```bash
dots-es-cli --config=local update-conf --rebuild
```

This applies the mappings shipped in `dots_es/elasticsearch/` to both indexes.

!!! danger "`--rebuild` deletes the index"
    The index is dropped before being recreated. Always means a full reindex afterwards.

## 4. Index

```bash
dots-es-cli --config=local index
```

The run prints a progress summary and ends with counts of collections, resources and passages, plus
the number of errors. Everything is also written to CSV files under `indexation_reporting/` — see
[Indexing reports](reporting.md).

To restrict the crawl to specific collections:

```bash
dots-es-cli --config=local index --collections=cartulaires
```

## 5. Check what landed in Elasticsearch

```bash
curl -X POST "http://localhost:9200/dots_document/_refresh?pretty"
curl "http://localhost:9200/_cat/indices?v"
```

With security enabled, prefix the host with credentials:

```bash
curl -X POST "http://elastic:$ES_PASSWORD@localhost:9200/dots_document/_refresh?pretty"
```

## 6. Search from the CLI

```bash
dots-es-cli --config=local search "cartulaire"
```

Or with a full Lucene query string:

```bash
dots-es-cli --config=local search -t "content:abbaye" --indexes=dots_document
```

## 7. Run the search API

```bash
dots-api --config=local
```

Then open:

```
http://localhost:5003/api/1.0/search?query=*&index=dots_document
```

The API listens on **port 5003** by default. See [Search API](search-api.md) for every query
parameter and the two response shapes.

## With Elasticsearch security enabled

`staging` and `prod` build their `ELASTICSEARCH_URL` from an environment variable, so prefix the
commands:

```bash
ES_PASSWORD=your_password dots-es-cli --config=prod index
```
