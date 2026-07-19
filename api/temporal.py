from functools import lru_cache


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

def build_temporal_aggs(temporal_fields: list[str]) -> dict:
    """
    Construit automatiquement les agrégations min/max.

    Exemple :

    temporal.dublincore.coverage

    =>
    coverage_min
    coverage_max
    """

    aggs = {}

    for field in temporal_fields:

        key = field.replace(".", "__")

        aggs[f"{key}_min"] = {
            "min": {
                "field": field + "_start"
            }
        }

        aggs[f"{key}_max"] = {
            "max": {
                "field": field + "_end"
            }
        }

    return aggs


def default_label(field: str) -> str:
    """
    Transforme un champ Elasticsearch en libellé technique.

    Exemples :
        temporal.dublincore.created
            -> dct:created

        temporal.extensions.datePublished
            -> schema:datePublished

        temporal.custom.myDate
            -> custom:myDate
    """

    parts = field.split(".")

    if len(parts) < 3:
        return field

    namespace = parts[-2]
    property_name = parts[-1]

    namespace_map = {
        "dublincore": "dct",
        "extensions": "schema",
    }

    prefix = namespace_map.get(namespace, namespace)

    return f"{prefix}:{property_name}"

def extract_temporal_facets(
    aggregations: dict,
    temporal_fields: list[str]
) -> list[dict]:
    """
    Reconstruit les facettes temporelles exploitables.

    Ignore automatiquement les champs absents
    (min/max = None).
    """

    facets = []

    for field in temporal_fields:

        key = field.replace(".", "__")

        min_value = aggregations[f"{key}_min"]["value"]
        max_value = aggregations[f"{key}_max"]["value"]

        if min_value is None or max_value is None:
            continue

        facets.append({
            "id": field.split(".")[-1],
            "label": default_label(field),

            # champ affiché/logique
            "field": field,

            # champs réellement utilisables dans une requête ES
            "start_field": field + "_start",
            "end_field": field + "_end",

            "min": int(min_value),
            "max": int(max_value)
        })

    return facets

