# Indexing reports

Every run writes CSV files to `indexation_reporting/`. The directory is gitignored except its README.

## File naming

All files of a single run share one UTC timestamp prefix with minute resolution, computed once when
the CLI starts:

```
2026_09_09_09_57_dots_document_indexation_exceptions.csv
2026_09_09_09_57_indexed_passages_report.csv
…
```

!!! warning "Two runs in the same minute share their files"
    The prefix has minute resolution and files are opened in append mode, so two runs started within
    the same minute write into the same files.

## The reports

| File | Columns | Written when |
|---|---|---|
| `{ts}_indexed_passages_report.csv` | `timestamp, resource_id, passage_id` | One row per passage successfully built. This is your reference count. |
| `{ts}_{DOCUMENT_INDEX}_indexation_exceptions.csv` | `timestamp, resource_id, passage_id, error_type, error_message, context` | A passage could not be built. **Also reused** for resource-level indexing failures, which write a shorter 5-column row — so this file can mix two row shapes. |
| `{ts}_{DOCUMENT_INDEX}_no_text.csv` | `timestamp, resource_id, passage_id, citeType, level, reason, context` | A fragment has neither `content` nor `head`, and was **skipped**. `reason` is always `NoIndexableText`. |
| `{ts}_{COLLECTION_INDEX}_indexation_exceptions.csv` | `timestamp, collection_id, error_type, error_message, context` | DTS collection fetch errors, JSONL write failures, and Elasticsearch collection indexing failures. |
| `{ts}_passage_exceptions.csv` | `timestamp, resource_id, passage_id, error_type, error_message, context` | JSON decode failures, bulk call failures, and **per-item Elasticsearch bulk rejections**. |
| `{ts}_metadata_dts_sanitization.csv` | `timestamp, collection_id, json_path, error_type, value` | Empty JSON keys found while sanitising a raw DTS response. |

!!! note "Two files are written without a header"
    Only four files get a header pre-written. `{ts}_passage_exceptions.csv` and
    `{ts}_metadata_dts_sanitization.csv` start directly with a data row — keep the column list above
    at hand when opening them.

A seventh file, `{ts}_dots_indexation_timing.csv`, is declared in the code but **never written**: the
helper has no call site. Treat it as reserved.

## What to check after a run

The CLI prints a summary — projects, sub-collections, resources, passages, excluded collections —
followed by counts read back from the CSV files, then the per-phase Elasticsearch durations.

A practical checklist:

1. **Passages en erreur ≠ 0** → open the document exceptions file and group by `error_type` and
   `context`.
2. **Passages sans texte ≠ 0** → open the `no_text` file. These fragments are silently *not* indexed.
   A large count usually means a mismatch between DTS navigation and the TEI.
3. **Always open `{ts}_passage_exceptions.csv`.** This is where Elasticsearch *rejections* land, and
   they do not stop the run. A real example from this project:

    ```
    failed to parse field [fragment_metadata.dublincore.date] of type [date] … '1154-12-16–1157'
    ```

    Rows here mean **data is missing from the index even though the crawl reported success** —
    typically mapping drift, where a dynamic `date` type was inferred from an earlier document.

4. **Collection exceptions** point at DTS availability problems rather than at your mappings.
5. **Cross-check the counts**: rows in `{ts}_indexed_passages_report.csv` versus the "passages"
   figure of the summary, versus `GET /dots_document/_count`.
6. **Watch stdout** for the `⚠️ Index … mapping 'dynamic=…' differs from conf` warning: it means the
   conf was never applied to that index.

!!! tip "The bulk error counter prints empty"
    The final summary has an *"Erreurs ES (bulk)"* line whose value is commented out in the code, so
    it always displays blank. Count the rows of `{ts}_passage_exceptions.csv` instead.
