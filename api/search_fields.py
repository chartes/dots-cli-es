# search_fields.py

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class SearchFieldType(str, Enum):
    KEYWORD = "keyword"
    TEXT = "text"
    TEMPORAL = "temporal"
    URL = "url"
    INTEGER = "integer"


class SearchFieldFamily(str, Enum):
    CLI = "cli"
    DTS = "dts"
    DCT = "dct"
    SCHEMA = "schema"
    DOTS = "dots"
    THUNDERDOTS = "thunderdots"


@dataclass(frozen=True)
class SearchField:

    id: str
    path: str

    family: SearchFieldFamily
    type: SearchFieldType

    index: bool = True

    facet: bool = False
    autocomplete: bool = False
    fulltext: bool = False
    multiple: bool = False

    range_start: Optional[str] = None
    range_end: Optional[str] = None

    @property
    def is_range_facet(self) -> bool:
        """
        Indique si le champ représente une facette temporelle range.

        Une facette range repose sur deux champs Elasticsearch :
        - un champ début
        - un champ fin

        Exemple :
            temporal.dublincore.coverage_start
            temporal.dublincore.coverage_end
        """
        return (
            self.facet
            and self.range_start is not None
            and self.range_end is not None
        )




SEARCH_FIELDS = [

    # ==========================================================
    # CLI / Elasticsearch
    # ==========================================================

    SearchField(
        "cli:parent",
        "parent_id",
        SearchFieldFamily.CLI,
        SearchFieldType.KEYWORD,
    ),

    SearchField(
        "cli:path",
        "path",
        SearchFieldFamily.CLI,
        SearchFieldType.KEYWORD,
    ),

    SearchField(
        "cli:pathIds",
        "path_ids",
        SearchFieldFamily.CLI,
        SearchFieldType.KEYWORD,
        multiple=True,
    ),

    SearchField(
        "cli:ancestors",
        "ancestors",
        SearchFieldFamily.CLI,
        SearchFieldType.KEYWORD,
        multiple=True,
    ),

    # ==========================================================
    # DTS
    # ==========================================================

    SearchField(
        "dts:id",
        "id",
        SearchFieldFamily.DTS,
        SearchFieldType.KEYWORD,
    ),

    SearchField(
        "dts:type",
        "type",
        SearchFieldFamily.DTS,
        SearchFieldType.KEYWORD,
    ),

    SearchField(
        "dts:title",
        "title",
        SearchFieldFamily.DTS,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dts:description",
        "description",
        SearchFieldFamily.DTS,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dts:download",
        "download",
        SearchFieldFamily.DTS,
        SearchFieldType.URL,
    ),

    SearchField(
        "content",
        "content",
        SearchFieldFamily.THUNDERDOTS,
        SearchFieldType.TEXT,
        fulltext=True,
    ),

    # ==========================================================
    # Dublin Core
    # ==========================================================

    SearchField(
        "dct:title",
        "dublincore.title",
        SearchFieldFamily.DCT,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dct:creator",
        "dublincore.creator",
        SearchFieldFamily.DCT,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "dct:created",
        "dublincore.created",
        SearchFieldFamily.DCT,
        SearchFieldType.TEMPORAL,
    ),

    SearchField(
        "dct:issued",
        "dublincore.issued",
        SearchFieldFamily.DCT,
        SearchFieldType.TEMPORAL,
    ),

    SearchField(
        "dct:coverage",
        "dublincore.coverage",
        SearchFieldFamily.DCT,
        SearchFieldType.TEMPORAL,
    ),

    SearchField(
        "dct:contributor",
        "dublincore.contributor",
        SearchFieldFamily.DCT,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "dct:publisher",
        "dublincore.publisher",
        SearchFieldFamily.DCT,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "dct:language",
        "dublincore.language",
        SearchFieldFamily.DCT,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "dct:description",
        "dublincore.description",
        SearchFieldFamily.DCT,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dct:source",
        "dublincore.source",
        SearchFieldFamily.DCT,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dct:isVersionOf",
        "dublincore.isVersionOf",
        SearchFieldFamily.DCT,
        SearchFieldType.URL,
    ),

    SearchField(
        "dct:rights",
        "dublincore.rights",
        SearchFieldFamily.DCT,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dct:license",
        "dublincore.license",
        SearchFieldFamily.DCT,
        SearchFieldType.URL,
    ),

    SearchField(
        "dct:relation",
        "dublincore.relation",
        SearchFieldFamily.DCT,
        SearchFieldType.URL,
    ),

    # ==========================================================
    # Schema.org
    # ==========================================================

    SearchField(
        "schema:name",
        "extensions.name",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "schema:author",
        "extensions.author",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "schema:editor",
        "extensions.editor",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "schema:publisher",
        "extensions.publisher",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),
    SearchField(
        "schema:dateCreated",
        "extensions.dateCreated",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.TEMPORAL,
    ),

    SearchField(
        "schema:datePublished",
        "extensions.datePublished",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.TEMPORAL,
    ),

    SearchField(
        "schema:temporalCoverage",
        "extensions.temporalCoverage",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.TEMPORAL,
    ),

    SearchField(
        "schema:description",
        "extensions.description",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "schema:license",
        "extensions.license",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.URL,
    ),

    SearchField(
        "schema:isBasedOn",
        "extensions.isBasedOn",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.URL,
    ),

    SearchField(
        "schema:exampleOfWork",
        "extensions.exampleOfWork",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.URL,
    ),

    SearchField(
        "schema:inLanguage",
        "extensions.inLanguage",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "schema:funder",
        "extensions.funder",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        facet=True,
        autocomplete=True,
        multiple=True,
    ),

    SearchField(
        "schema:associatedMedia",
        "extensions.associatedMedia",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        multiple=True,
    ),

    SearchField(
        "schema:subjectOf",
        "extensions.subjectOf",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        multiple=True,
    ),

    SearchField(
        "schema:about",
        "extensions.about",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
        multiple=True,
    ),

    SearchField(
        "schema:@type",
        "extensions.@type",
        SearchFieldFamily.SCHEMA,
        SearchFieldType.KEYWORD,
    ),

    # ==========================================================
    # DoTS extensions
    # ==========================================================

    SearchField(
        "dots:shortTitle",
        "extensions.dots:shortTitle",
        SearchFieldFamily.DOTS,
        SearchFieldType.TEXT,
    ),

    SearchField(
        "dots:resourceIIIFManifest",
        "extensions.dots:resourceIIIFManifest",
        SearchFieldFamily.DOTS,
        SearchFieldType.URL,
    ),

    # ==========================================================
    # Temporal range facets (generated by Thunderdots)
    # ==========================================================

    SearchField(
        id="dct:created:range",
        path="temporal.dublincore.created",
        range_start="temporal.dublincore.created_start",
        range_end="temporal.dublincore.created_end",
        family=SearchFieldFamily.DCT,
        type=SearchFieldType.TEMPORAL,
        facet=True,
    ),

    SearchField(
        id="dct:issued:range",
        path="temporal.dublincore.issued",
        range_start="temporal.dublincore.issued_start",
        range_end="temporal.dublincore.issued_end",
        family=SearchFieldFamily.DCT,
        type=SearchFieldType.TEMPORAL,
        facet=True,
    ),

    SearchField(
        id="dct:coverage:range",
        path="temporal.dublincore.coverage",
        range_start="temporal.dublincore.coverage_start",
        range_end="temporal.dublincore.coverage_end",
        family=SearchFieldFamily.DCT,
        type=SearchFieldType.TEMPORAL,
        facet=True,
    ),

    SearchField(
        id="schema:dateCreated:range",
        path="temporal.extensions.dateCreated",
        range_start="temporal.extensions.dateCreated_start",
        range_end="temporal.extensions.dateCreated_end",
        family=SearchFieldFamily.SCHEMA,
        type=SearchFieldType.TEMPORAL,
        facet=True,
    ),

    SearchField(
        id="schema:datePublished:range",
        path="temporal.extensions.datePublished",
        range_start="temporal.extensions.datePublished_start",
        range_end="temporal.extensions.datePublished_end",
        family=SearchFieldFamily.SCHEMA,
        type=SearchFieldType.TEMPORAL,
        facet=True,
    ),

    SearchField(
        id="schema:temporalCoverage:range",
        path="temporal.extensions.temporalCoverage",
        range_start="temporal.extensions.temporalCoverage_start",
        range_end="temporal.extensions.temporalCoverage_end",
        family=SearchFieldFamily.SCHEMA,
        type=SearchFieldType.TEMPORAL,
        facet=True,
    ),

]

# SEARCH_FIELDS = [
#
#     # ==========================================================
#     # CLI / Elasticsearch (générés par le CLI)
#     # ==========================================================
#
#     SearchField("cli:parent",      "parent_id",  "cli", "keyword"),
#     SearchField("cli:path",        "path",       "cli", "keyword"),
#     SearchField("cli:pathIds",     "path_ids",   "cli", "keyword"),
#     SearchField("cli:ancestors",   "ancestors",  "cli", "keyword"),
#
#
#     # ==========================================================
#     # DTS (racine)
#     # ==========================================================
#
#     SearchField("dts:id",             "id",             "dts", "keyword"),
#     SearchField("dts:type",           "type",           "dts", "keyword"),
#
#     SearchField("dts:title",          "title",          "dts", "text"),
#     SearchField("dts:description",    "description",    "dts", "text"),
#
#     # SearchField("dts:dtsVersion",     "dtsVersion",     "dts", "keyword"),
#
#     # SearchField("dts:totalParents",   "totalParents",   "dts", "integer"),
#     # SearchField("dts:totalChildren",  "totalChildren",  "dts", "integer"),
#
#     SearchField("dts:download",       "download",       "dts", "url"),
#
#     # SearchField("dts:collection",     "collection",     "dts", "keyword"),
#     # SearchField("dts:navigation",     "navigation",     "dts", "keyword"),
#     # SearchField("dts:document",       "document",       "dts", "keyword"),
#
#     # SearchField("dts:citationTrees",  "citationTrees",  "dts", "keyword"),
#     # SearchField("dts:mediaTypes",     "mediaTypes",     "dts", "keyword"),
#
#     # SearchField("dts:@context",       "@context",       "dts", "keyword"),
#
#     # texte intégral produit par thunderdots
#     SearchField(
#         "content",
#         "content",
#         "thunderdots",
#         "text",
#         fulltext=True
#     ),
#
#
#     # ==========================================================
#     # Dublin Core
#     # ==========================================================
#
#     SearchField("dct:title",                    "dublincore.title",                    "dct", "text"),
#     SearchField("dct:creator",                  "dublincore.creator",                  "dct", "keyword", facet=True),
#
#     SearchField("dct:created",                  "dublincore.created",                  "dct", "temporal"),
#     SearchField("dct:issued",                   "dublincore.issued",                   "dct", "temporal"),
#     SearchField("dct:coverage",                 "dublincore.coverage",                 "dct", "temporal"),
#
#     SearchField("dct:contributor",              "dublincore.contributor",              "dct", "keyword", facet=True),
#     SearchField("dct:publisher",                "dublincore.publisher",                "dct", "keyword", facet=True),
#     SearchField("dct:language",                 "dublincore.language",                 "dct", "keyword", facet=True),
#
#     SearchField("dct:description",              "dublincore.description",              "dct", "text"),
#     SearchField("dct:source",                   "dublincore.source",                   "dct", "text"),
#
#     # SearchField("dct:identifier",               "dublincore.identifier",               "dct", "url"),
#     SearchField("dct:isVersionOf",              "dublincore.isVersionOf",              "dct", "url"),
#
#     SearchField("dct:rights",                   "dublincore.rights",                   "dct", "text"),
#     SearchField("dct:license",                  "dublincore.license",                  "dct", "url"),
#
#     SearchField("dct:relation",                 "dublincore.relation",                 "dct", "url"),
#
#     # SearchField("dct:bibliographicCitation",    "dublincore.bibliographicCitation",    "dct", "text"),
#
#
#     # ==========================================================
#     # Schema.org Extensions
#     # ==========================================================
#
#     SearchField("schema:name",                  "extensions.name",                  "schema", "text"),
#
#     SearchField("schema:author",                "extensions.author",                "schema", "keyword", facet=True),
#     SearchField("schema:editor",                "extensions.editor",                "schema", "keyword", facet=True),
#     SearchField("schema:publisher",             "extensions.publisher",             "schema", "keyword", facet=True),
#
#     SearchField("schema:dateCreated",           "extensions.dateCreated",           "schema", "temporal"),
#     SearchField("schema:datePublished",         "extensions.datePublished",         "schema", "temporal"),
#     SearchField("schema:temporalCoverage",      "extensions.temporalCoverage",      "schema", "temporal"),
#
#     SearchField("schema:description",           "extensions.description",           "schema", "text"),
#
#     SearchField("schema:license",               "extensions.license",               "schema", "url"),
#     SearchField("schema:isBasedOn",             "extensions.isBasedOn",             "schema", "url"),
#     SearchField("schema:exampleOfWork",         "extensions.exampleOfWork",         "schema", "url"),
#
#     SearchField("schema:inLanguage",            "extensions.inLanguage",            "schema", "keyword", facet=True),
#
#     SearchField("schema:funder",                "extensions.funder",                "schema", "keyword", facet=True),
#
#     # SearchField("schema:encoding",              "extensions.encoding",              "schema", "keyword"),
#     SearchField("schema:associatedMedia",       "extensions.associatedMedia",       "schema", "keyword"),
#     SearchField("schema:subjectOf",             "extensions.subjectOf",             "schema", "keyword"),
#     SearchField("schema:about",                 "extensions.about",                 "schema", "keyword"),
#
#     # SearchField("schema:creditText",            "extensions.creditText",            "schema", "text"),
#
#     # SearchField("schema:@context",              "extensions.@context",              "schema", "keyword"),
#     SearchField("schema:@type",                 "extensions.@type",                 "schema", "keyword"),
#
#
#     # ==========================================================
#     # DoTS extensions
#     # ==========================================================
#
#     SearchField(
#         "dots:shortTitle",
#         "extensions.dots:shortTitle",
#         "dots",
#         "text"
#     ),
#
#     SearchField(
#         "dots:resourceIIIFManifest",
#         "extensions.dots:resourceIIIFManifest",
#         "dots",
#         "url"
#     ),
#]

# ----------------------------------------------------------------------
# Lookup tables
# ----------------------------------------------------------------------

SEARCH_FIELDS_BY_ID = {
    field.id: field
    for field in SEARCH_FIELDS
}


SEARCH_FIELDS_BY_PATH = {
    field.path: field
    for field in SEARCH_FIELDS
}


def get_search_field(id_or_path: str) -> Optional[SearchField]:
    """
    Lookup by id first, then by path.
    """
    return (
        SEARCH_FIELDS_BY_ID.get(id_or_path)
        or SEARCH_FIELDS_BY_PATH.get(id_or_path)
    )


# ----------------------------------------------------------------------
# Field groups
# ----------------------------------------------------------------------

def indexed_fields():
    """
    Champs réellement indexés dans les métadonnées de recherche.

    Les facettes temporelles range ne sont pas indexées ici :
    elles utilisent directement les champs temporal.*_start/end
    produits par Thunderdots.
    """
    return [
        field
        for field in SEARCH_FIELDS
        if field.index
        and not field.is_range_facet
    ]


def facet_fields():
    """
    Facettes classiques (keyword, listes, etc.).

    Exclut les facettes temporelles range.
    """
    return [
        field
        for field in SEARCH_FIELDS
        if field.facet
        and not field.is_range_facet
    ]


def range_facet_fields():
    """
    Facettes temporelles utilisant un couple start/end.
    """
    return [
        field
        for field in SEARCH_FIELDS
        if field.is_range_facet
    ]


def fulltext_fields():
    return [
        field
        for field in SEARCH_FIELDS
        if field.fulltext
    ]


def temporal_fields():
    """
    Champs temporels métier.

    Exemple :
        dct:created
        schema:datePublished

    Ce ne sont pas les champs utilisés pour les ranges.
    """
    return [
        field
        for field in SEARCH_FIELDS
        if field.type == SearchFieldType.TEMPORAL
        and not field.is_range_facet
    ]


def fields_by_family(
    family: SearchFieldFamily
):
    return [
        field
        for field in SEARCH_FIELDS
        if field.family == family
    ]


def fields_by_type(
    type_: SearchFieldType
):
    return [
        field
        for field in SEARCH_FIELDS
        if field.type == type_
    ]


# ----------------------------------------------------------------------
# Metadata extraction
# ----------------------------------------------------------------------

def get_value(
    document: dict,
    field: SearchField
):
    """
    Retourne la valeur correspondant au path d'un SearchField.
    """

    value = document

    for part in field.path.split("."):

        if not isinstance(value, dict):
            return None

        value = value.get(part)

        if value is None:
            return None

    return value

# ----------------------------------------------------------------------
# Metadata facets helpers
# ----------------------------------------------------------------------

def build_searchfield_aggs():
    aggs = {}

    for field in SEARCH_FIELDS:
        if not field.facet or field.is_range_facet:
            continue

        if field.type != SearchFieldType.KEYWORD:
            continue

        aggs[field.id] = {
            "terms": {
                "field": get_es_field(field),
                "size": 10000
            },
            "aggs": {
                "resource_count": {
                    "cardinality": {
                        "field": "resource_id",
                        "precision_threshold": 40000
                    }
                }
            }
        }

    return aggs

def get_facet_es_field(facet_id):

    # Facette spéciale collections
    if facet_id == "collections":
        return "collection_facets"

    # Toutes les autres facettes configurées
    for field in SEARCH_FIELDS:

        if field.id == facet_id:

            return get_es_field(field)

    raise ValueError(
        f"Unknown facet field {facet_id}"
    )

def extract_searchfield_facets(aggregations):
    facets = {}

    for field in SEARCH_FIELDS:
        if not field.facet or field.is_range_facet:
            continue

        buckets = aggregations.get(field.id, {}).get("buckets", [])

        facets[field.id] = [
            {
                "value": bucket["key"],
                "count": bucket["resource_count"]["value"]
            }
            for bucket in buckets
        ]

    return facets


def get_es_field(field: SearchField) -> str:
    if field.family in (
        SearchFieldFamily.DCT,
        SearchFieldFamily.SCHEMA,
        SearchFieldFamily.DOTS,
    ):
        path = f"resource_metadata.{field.path}"
    else:
        path = field.path

    if field.type == SearchFieldType.KEYWORD:
        path += ".keyword"

    return path



# ----------------------------------------------------------------------
# Temporal facets helpers
# ----------------------------------------------------------------------

def build_temporal_aggs(
    fields: list[SearchField] | None = None
) -> dict:
    """
    Construit les agrégations min/max pour les facettes temporelles.

    Les champs sont déclarés dans SEARCH_FIELDS via :
        range_start
        range_end

    Exemple :

        range_start:
            temporal.dublincore.coverage_start

        range_end:
            temporal.dublincore.coverage_end

    Produit :

        coverage_min
        coverage_max
    """

    if fields is None:
        fields = range_facet_fields()

    aggs = {}

    for field in fields:

        key = field.id.replace(":", "__")

        aggs[f"{key}_min"] = {
            "min": {
                "field": field.range_start
            }
        }

        aggs[f"{key}_max"] = {
            "max": {
                "field": field.range_end
            }
        }

    return aggs


def default_label(
    field: SearchField
) -> str:
    """
    Retourne un label technique exploitable côté UI.

    Exemple :

        dct:created:range
            -> dct:created

        schema:datePublished:range
            -> schema:datePublished
    """

    if field.id.endswith(":range"):
        return field.id[:-6]

    return field.id


def extract_temporal_facets(
    aggregations: dict,
    fields: list[SearchField] | None = None,
) -> list[dict]:
    """
    Reconstruit les facettes temporelles exploitables par le front.

    Ignore les champs sans valeurs min/max.
    """

    if fields is None:
        fields = range_facet_fields()

    facets = []

    for field in fields:

        key = field.id.replace(":", "__")

        min_bucket = aggregations.get(
            f"{key}_min",
            {}
        )

        max_bucket = aggregations.get(
            f"{key}_max",
            {}
        )

        min_value = min_bucket.get("value")
        max_value = max_bucket.get("value")

        if min_value is None or max_value is None:
            continue

        facets.append({

            # identifiant front
            "id": field.id,

            # label humain
            "label": default_label(field),

            # groupe logique
            "field": field.path,

            # champs ES réellement filtrables
            "start_field": field.range_start,
            "end_field": field.range_end,

            "min": int(min_value),
            "max": int(max_value),
        })

    return facets

def build_filtered_temporal_metadata(
    temporal_metadata: dict,
) -> dict:
    """
    Filtre le temporal produit par Thunderdots.

    Le temporal Thunderdots est sans préfixe "temporal.".
    Le contrat SearchField utilise les chemins ES complets.

    Garde uniquement :
    - les champs range déclarés dans SEARCH_FIELDS
    - leurs champs _start / _end

    Supprime :
    - les champs temporels bruts
    - les champs *_iso
    - les artefacts extensions.@context
    """

    allowed = {}

    range_fields = {
        field.path: field
        for field in SEARCH_FIELDS
        if field.is_range_facet
    }

    for logical_path, field in range_fields.items():

        for source_path, target_path in (
            (field.range_start, field.range_start),
            (field.range_end, field.range_end),
        ):

            if not source_path:
                continue

            # SearchField :
            # temporal.dublincore.created_start
            #
            # Thunderdots :
            # dublincore.created_start
            thunderdots_key = source_path.removeprefix(
                "temporal."
            )

            value = temporal_metadata.get(
                thunderdots_key
            )

            if value is not None:
                allowed[target_path] = value

    return allowed