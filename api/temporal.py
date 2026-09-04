import logging

from functools import lru_cache

from .search_fields import (
    metadata_key_from_path,
    range_field_by_es_path
)

logger = logging.getLogger(__name__)


@lru_cache(maxsize=8)
def get_temporal_fields(es, index: str) -> list[str]:
    """
    Découvre automatiquement les champs temporels disposant
    d'un couple _start / _end numérique.

    Retour :
    [
        "temporal.dublincore.coverage",
        "temporal.dublincore.created",
        "temporal.extensions.dateCreated",
        ...
    ]
    """

    mapping = es.indices.get_field_mapping(
        index=index,
        fields="temporal.*"
    )

    fields = mapping[index]["mappings"]

    temporal_fields = []

    for field, definition in fields.items():

        if not field.endswith("_start"):
            continue

        logical_field = field[:-6]          # retire "_start"
        end_field = logical_field + "_end"

        if end_field not in fields:
            continue

        start_type = definition.get("mapping", {})
        end_type = fields[end_field].get("mapping", {})

        # Récupération du type ES
        start_mapping = next(iter(start_type.values()), {})
        end_mapping = next(iter(end_type.values()), {})

        start_es_type = start_mapping.get("type")
        end_es_type = end_mapping.get("type")

        # On ignore les champs texte/date string
        if start_es_type not in (
            "integer",
            "long",
            "short",
            "byte",
            "double",
            "float"
        ):
            continue

        if end_es_type != start_es_type:
            continue

        temporal_fields.append(logical_field)

    return sorted(temporal_fields)


@lru_cache(maxsize=64)
def temporal_key(field: str) -> str:
    """
    Canonical metadata key of a temporal field, resolved through
    SEARCH_FIELDS rather than derived from the last path segment.

    Truncating collapsed distinct properties -- `temporal.dublincore.created`
    and `temporal.extensions.created` both became "created". The namespaced
    key keeps them apart.
    """
    registry_field = range_field_by_es_path(field)

    if registry_field is not None:
        return registry_field.key

    # Temporal fields are discovered from the mapping, so a field may
    # legitimately have no registry entry yet. Fall back on the same
    # derivation and flag it: it means SEARCH_FIELDS needs an entry.
    key = metadata_key_from_path(field)

    logger.warning(
        "Temporal field %r is not declared in SEARCH_FIELDS; "
        "derived its key as %r.",
        field,
        key
    )

    return key


def build_temporal_aggs(
    temporal_fields,
    base_must,
    base_filters,
    ranges
):
    aggs = {}

    for field in temporal_fields:

        key = field.replace(".", "__")

        start_field = field + "_start"
        end_field = field + "_end"

        # Toutes les ranges SAUF celles qui concernent
        # la facette temporelle courante.
        facet_ranges = []

        for range_query in ranges:
            if start_field in range_query:
                continue

            if end_field in range_query:
                continue

            facet_ranges.append({
                "range": range_query
            })

        facet_bool = {
            "must": list(base_must),
            "filter": list(base_filters) + facet_ranges
        }

        aggs[f"{key}_available"] = {
            "global": {},
            "aggs": {
                "filtered": {
                    "filter": {
                        "bool": facet_bool
                    },
                    "aggs": {

                        # Enveloppe globale des plages
                        "min": {
                            "min": {
                                "field": start_field
                            }
                        },
                        "max": {
                            "max": {
                                "field": end_field
                            }
                        },

                        # # Intersection commune
                        # "intersection_min": {
                        #     "max": {
                        #         "field": start_field
                        #     }
                        # },
                        # "intersection_max": {
                        #     "min": {
                        #         "field": end_field
                        #     }
                        # }
                    }
                }
            }
        }

    return aggs


def unflatten_dict(data):
    result = {}

    for key, value in data.items():
        parts = key.split(".")
        current = result

        for part in parts[:-1]:
            current = current.setdefault(part, {})

        current[parts[-1]] = value

    return result


def extract_temporal_facets(
    aggregations: dict,
    temporal_fields: list[str]
) -> list[dict]:

    facets = []

    for field in temporal_fields:

        key = field.replace(".", "__")

        available = aggregations.get(
            f"{key}_available"
        )

        if not available:
            continue

        filtered = available.get("filtered")

        if not filtered:
            continue

        min_value = filtered["min"]["value"]
        max_value = filtered["max"]["value"]

        if min_value is None or max_value is None:
            continue

        # intersection_min = filtered["intersection_min"]["value"]
        # intersection_max = filtered["intersection_max"]["value"]
        #
        # intersection = None
        #
        # if (
        #     intersection_min is not None
        #     and intersection_max is not None
        #     and intersection_min <= intersection_max
        # ):
        #     intersection = {
        #         "min": int(intersection_min),
        #         "max": int(intersection_max)
        #     }

        facets.append({
            "key": temporal_key(field),
            # Fallback label: the canonical key itself, so a collection with
            # no configured label displays exactly the string an editor has
            # to paste into searchConfig.temporalFacets to customise it.
            # Term facets already fall back the same way, on their key.
            "label": temporal_key(field),
            "field": field,
            "start_field": field + "_start",
            "end_field": field + "_end",
            "min": int(min_value),
            "max": int(max_value),
        })#"intersection": intersection

    return facets


def build_open_range(range_query):
    field, condition = next(iter(range_query.items()))

    return {
        "bool": {
            "should": [
                {
                    "range": {
                        field: condition
                    }
                },
                {
                    "bool": {
                        "must_not": [
                            {
                                "exists": {
                                    "field": field
                                }
                            }
                        ]
                    }
                }
            ],
            "minimum_should_match": 1
        }
    }



