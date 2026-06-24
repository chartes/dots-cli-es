import json
import pprint
import time
from typing import Callable
from flask import Response, request, current_app

def parse_filters_param(filters_param: str):
    """
    Parse un paramètre filters de type:
    field1:val1|val2,field2:val3

    Retourne une liste de filtres ES (term / terms)
    """
    es_filters = []

    if not filters_param:
        return es_filters

    for part in filters_param.split(","):
        if ":" not in part:
            continue

        field, raw_values = part.split(":", 1)
        field = field.strip()
        values = [v.strip() for v in raw_values.split("|") if v.strip()]

        if not values:
            continue

        # cast simple (int si possible)
        casted_values = []
        for v in values:
            if v.isdigit():
                casted_values.append(int(v))
            else:
                casted_values.append(v)

        # champ numérique → pas de .keyword
        is_numeric = all(isinstance(v, int) for v in casted_values)
        es_field = field if is_numeric else f"{field}.keyword"

        if len(casted_values) == 1:
            es_filters.append({
                "term": { es_field: casted_values[0] }
            })
        else:
            es_filters.append({
                "terms": { es_field: casted_values }
            })

    return es_filters

def parse_range_parameter():
    _range = None
    _parsed_ranges = []
    for f in request.args.keys():
        if f.startswith('range[') and f.endswith(']'):
            key, ops = (f[len('range['):-1], [op.split(':') for op in request.args[f].split(",")])
            print(key, ops)
            _range = {key: {}}
            for op, value in ops:
                _range[key][op] = value
            _parsed_ranges.append(_range)
    return _parsed_ranges


def parse_query_param(query_param: str):
    """
    Parse le paramètre query du type:
    - Sans ':', recherche sur une liste de champs spécifiques.
    - Avec ':', recherche spécifique sur le champ indiqué.

    Retourne une liste de clauses ES pour 'must', gère les wildcards
    et applique la bonne logique pour text vs keyword.

    :param query_param: La valeur de la requête à analyser (par exemple, 'recuperandum').
    """
    # Définir une liste par défaut de champs à rechercher
    fields_to_search = ['title', 'content']

    clauses = []
    if not query_param:
        return clauses

    # Si la query ne contient pas de ':', on recherche sur les champs spécifiés
    if ':' not in query_param:
        # Si la valeur contient des wildcards, on crée une clause query_string
        if "*" in query_param or "?" in query_param:
            fields_query = " OR ".join([f"{field}:{query_param}" for field in fields_to_search])
            clauses.append({
                "query_string": {
                    "query": fields_query,
                    "default_operator": "AND"
                }
            })
        else:
            clauses.append({
                "bool": {
                    "should": [
                        {
                            "match_phrase_prefix": {
                                field: query_param
                            }
                        }
                        for field in fields_to_search
                    ],
                    "minimum_should_match": 1  # Au moins un match dans l'un des champs
                }
            })
    else:
        # Si la query contient un ':', on sépare le champ et la valeur
        for part in query_param.split(","):
            print('part : ', part)
            part = part.strip()

            if not part or ":" not in part:
                continue

            field, value = part.split(":", 1)
            field = field.strip()
            value = value.strip().strip('"')

            # Gère les wildcards
            if "*" in value or "?" in value:
                clauses.append({
                    "query_string": {
                        "query": f"{field}:{value}",
                        "default_operator": "AND"
                    }
                })
            else:
                clauses.append({
                    "match_phrase_prefix": {
                        field: value
                    }
                })

    return clauses

def register_search_endpoint(
    app,
    api_version="1.0",
    compose_result_func: Callable[[str], list] = lambda s: [],
    compose_result_grouped_by_resource: Callable[[str], list] = lambda s: []
):
    @app.route(f"/api/{api_version}/search", methods=["GET"])
    def api_search_endpoint():
        start_time: float = time.time()
        index: str = request.args.get("index", None)
        query_param: str = request.args.get("query", None)
        ranges: list[dict] = parse_range_parameter()
        filters_param = request.args.get("filters")
        groupby_field: str = request.args.get("groupby[field]", None)
        groupby_after: str = request.args.get("groupby[after-page]", None)
        groupby_with_ids: int = request.args.get("groupby[with-ids]", 10000) or 10000
        collection_id: str = request.args.get("collectionId")
        collection_facet = request.args.get("collection_facet")

        no_highlight = isinstance(request.args.get("no-highlight", False), str)

        # Pagination
        num_page = int(request.args.get('page[number]', 1))
        page_size = max(int(request.args.get('page[size]', current_app.config["SEARCH_RESULT_PER_PAGE"])), 10)

        # Tri
        sort_criteriae: list[dict] = []
        if "sort" in request.args:
            for criteria in request.args["sort"].split(','):
                sort_order = "asc"
                if criteria.startswith('-'):
                    sort_order = "desc"
                    criteria = criteria[1:]
                sort_criteriae.append({criteria: {"order": sort_order}})

        r = {}
        try:
            if index is None or len(index) == 0:
                index = current_app.config["DOCUMENT_INDEX"]

            # === CAS 1 : Recherche simple sur ressources filtrée par collection ===
            if not query_param and collection_id:
                print('\nsearch CAS 1')
                body_query = {
                    "query": {
                        "bool": {
                            "must": [{"term": {"type.keyword": "Resource"}}],
                            "filter": [{"term": {"resource_metadata.path_ids.keyword": collection_id}}]
                        }
                    },
                    "_source": [
                        "resource_metadata.dublincore.title",
                        "resource_metadata.dublincore.creator",
                        "resource_metadata.dublincore.date",
                        "resource_metadata.dublincore.coverage"
                    ],
                    "sort": sort_criteriae,
                    "from": (num_page - 1) * page_size,
                    "size": page_size,
                    "track_total_hits": True,
                    "aggregations": {
                        "collections_fac": {
                            "terms": {
                                "field": "collection_facets",
                                "size": 100000
                            }
                        }
                    }
                }
                if collection_facet:
                    body_query["query"]["bool"].setdefault("filter", []).append({
                        "term": {
                            "collection_facets": collection_facet
                        }
                    })

                search_result = current_app.elasticsearch.search(index=index, body=body_query)
                print('\nbody_query')
                print(body_query)


                print('\nsearch_result["aggregations"]')
                print(search_result["aggregations"])
                collection_facets = []

                for bucket in search_result["aggregations"]["collections_fac"]["buckets"]:
                    try:
                        coll_id, label = bucket["key"].split("###", 1)
                    except ValueError:
                        coll_id = bucket["key"]
                        label = bucket["key"]

                    collection_facets.append({
                        "id": coll_id,
                        "label": label,
                        "count": bucket["doc_count"],
                        "facet_key": bucket["key"]
                    })

                results = [
                    {
                        "resource_id": hit["_id"],
                        "title": hit["_source"].get("resource_metadata", {}).get("dublincore", {}).get("title"),
                        "creator": hit["_source"].get("resource_metadata", {}).get("dublincore", {}).get("creator"),
                        "date": hit["_source"].get("resource_metadata", {}).get("dublincore", {}).get("created"),
                        "coverage": hit["_source"].get("resource_metadata", {}).get("dublincore", {}).get("coverage")
                    }
                    for hit in search_result["hits"]["hits"]
                ]

                r = {
                    "data": results,
                    "total_count": search_result["hits"]["total"]["value"],
                    "facets": {
                        "collections": collection_facets
                    }
                }

            # === CAS 2 : Recherche groupée par resource (hits + highlights dans la même requête) ===
            elif groupby_field:
                print('\nsearch CAS 2')
                # Corps principal de la requête
                body_query = {
                    "query": {
                        "bool": {
                            "must": [{"match_all": {}}]
                        }
                    },
                    "sort": sort_criteriae,
                    "track_total_hits": True,
                    "track_scores": True,
                    "size": page_size if not no_highlight else 0,  # récupérer hits seulement si highlight demandé
                    "highlight": {} if no_highlight else {
                        "type": "unified",
                        "require_field_match": False,
                        "pre_tags": ["<mark>"],
                        "post_tags": ["</mark>"],
                        "fields": {
                            "content": {
                                "fragment_size": 50,
                                "number_of_fragments": 100,
                                "no_match_size": 50
                            }
                        }
                    },
                    "aggregations": {
                        "resources": {
                            "composite": {
                                "sources": [{groupby_field: {"terms": {"field": groupby_field}}}],
                                "size": page_size
                            }
                        },
                        "bucket_count": {"cardinality": {"field": groupby_field}},
                        "collections": {
                            "terms": {
                                "field": "collection_facets",
                                "size": 1000
                            }
                        }
                    },
                }

                # Ajouter la clause full-text
                if query_param:
                    body_query["query"]["bool"]["must"].extend(parse_query_param(query_param))
                else:
                    body_query["query"]["bool"]["must"].append({"match_all": {}})

                # Ajouter les ranges
                if ranges:
                    body_query["query"]["bool"]["must"].extend([{"range": r} for r in ranges])

                # Ajouter les filtres
                if filters_param:
                    es_filters = parse_filters_param(filters_param)
                    if es_filters:
                        body_query["query"]["bool"].setdefault("filter", []).extend(es_filters)

                if collection_facet:
                    body_query["query"]["bool"].setdefault("filter", []).append({
                        "term": {
                            "collection_facets": collection_facet
                        }
                    })

                # Pagination après pour composite
                if groupby_after:
                    sources_keys = [list(s.keys())[0] for s in
                                    body_query["aggregations"]["resources"]["composite"]["sources"]]
                    body_query["aggregations"]["resources"]["composite"]["after"] = {
                        key: value for key, value in zip(sources_keys, groupby_after.split(','))
                    }

                print('body_query', body_query)
                search_result = current_app.elasticsearch.search(index=index, body=body_query)
                #print('search_result', search_result)

                from collections import Counter
                # extraire toutes les resource_id
                resource_ids = [
                    hit["_source"].get("resource_id")
                    for hit in search_result["hits"]["hits"]
                    if hit["_source"].get("resource_id")
                ]
                # compter le nombre de hits par resource_id
                counts = Counter(resource_ids)
                # afficher
                for resource_id, count in counts.most_common():
                    print(f"{resource_id}: {count} hits")

                # Récupération des buckets
                buckets = search_result["aggregations"]["resources"]["buckets"]
                bucket_count = search_result["aggregations"]["bucket_count"]["value"]
                after_key = search_result["aggregations"]["resources"].get("after_key", None)

                collection_facets = []

                for bucket in search_result["aggregations"]["collections"]["buckets"]:
                    try:
                        coll_id, label = bucket["key"].split("###", 1)
                    except ValueError:
                        coll_id = bucket["key"]
                        label = bucket["key"]

                    collection_facets.append({
                        "id": coll_id,
                        "label": label,
                        "count": bucket["doc_count"],
                        "facet_key": bucket["key"]
                    })

                if collection_facet:
                    collection_facets = [
                        f for f in collection_facets
                        if f["facet_key"] != collection_facet
                    ]


                # Construire le résultat groupé directement depuis les hits
                def add_ellipsis(fragment):
                    if not fragment:
                        return fragment

                    text = fragment.strip()

                    # Ajouter "..." au début si ça ne commence pas par une majuscule (souvent milieu de phrase)
                    if text and text[0].islower():
                        text = "..." + text

                    # Ajouter "..." à la fin si pas de ponctuation finale
                    if not text.endswith((".", "…", "!", "?")):
                        text = text + "..."

                    return text

                grouped_results = []
                for b in buckets:
                    # Sélectionner les hits correspondant à ce resource_id
                    hits_list = [
                        h for h in search_result["hits"]["hits"]
                        if h["_source"].get(groupby_field) == b["key"][groupby_field]
                    ]
                    if not hits_list:
                        continue

                    first_hit_source = hits_list[0]["_source"]

                    grouped_results.append({
                        "resource_id": b["key"][groupby_field],
                        "title": first_hit_source.get("resource_metadata", {}).get("dublincore", {}).get("title"),
                        "creator": first_hit_source.get("resource_metadata", {}).get("dublincore", {}).get("creator"),
                        "date": first_hit_source.get("resource_metadata", {}).get("dublincore", {}).get("created"),
                        "collection_ids": list({
                            c.get("collection_id")
                            for c in first_hit_source.get("collections", [])
                            if c.get("collection_id")
                        }),
                        "hits": [
                            {
                                "passage_id": h["_source"].get("passage_id"),
                                "title": h["_source"].get("title"),
                                "level": h["_source"].get("level", 1),
                                "ancestors": h["_source"].get("ancestors", []),
                                "citeType": h["_source"].get("citeType"),
                                "highlight": None if no_highlight else {
                                    "content": [
                                        add_ellipsis(frag)
                                        for frag in (h.get("highlight", {}).get("content") or [])
                                    ]
                                }
                            } for h in hits_list
                        ]
                    })

                # r = {
                #     "buckets": grouped_results,
                #     "after_key": after_key,
                #     "bucket_count": bucket_count,
                #     "total_count": search_result["hits"]["total"]["value"]
                # }
                r = {
                    "buckets": grouped_results,

                    "facets": {
                        "collections": collection_facets
                    },

                    "after_key": after_key,
                    "bucket_count": bucket_count,
                    "total_count": search_result["hits"]["total"]["value"]
                }

            # === CAS 3 : Recherche classique avec query_param ===
            else:
                print('\nsearch CAS 3')
                body_query = {
                    "query": {
                        "bool": {
                            "must": [{"match_all": {}}]
                        }
                    },
                    "sort": sort_criteriae,
                    "track_total_hits": True,
                    "track_scores": True,
                    "aggregations": {
                        "collections": {
                            "terms": {
                                "field": "collection_facets",
                                "size": 1000
                            }
                        }
                    }
                }

                # Toujours filtrer type=Resource si collection_id fourni
                # if collection_id:
                #     body_query["query"]["bool"].setdefault("filter", []).append({"term": {"type": "Resource"}})

                if query_param:
                    body_query["query"]["bool"]["must"].extend(parse_query_param(query_param))
                else:
                    body_query["query"]["bool"]["must"].append({"match_all": {}})

                if ranges:
                    body_query["query"]["bool"]["must"].extend([{"range": r} for r in ranges])

                if filters_param:
                    es_filters = parse_filters_param(filters_param)
                    if es_filters:
                        body_query["query"]["bool"].setdefault("filter", []).extend(es_filters)

                body_query["from"] = (num_page - 1) * page_size
                body_query["size"] = page_size

                print('\nbody_query')
                print(body_query)
                search_result = current_app.elasticsearch.search(index=index, body=body_query)
                results = compose_result_func(search_result)

                collection_facets = []

                print('\nsearch_result["aggregations"]')
                print(search_result["aggregations"])
                for bucket in search_result["aggregations"]["collections"]["buckets"]:
                    try:
                        coll_id, label = bucket["key"].split("###", 1)
                    except:
                        coll_id = bucket["key"]
                        label = bucket["key"]

                    collection_facets.append({
                        "id": coll_id,
                        "label": label,
                        "count": bucket["doc_count"],
                        "facet_key": bucket["key"]
                    })

                r = {
                    "data": results,
                    "facets": {
                        "collections": collection_facets
                    },
                    "total_count": search_result["hits"]["total"]["value"]
                }

                # r = {
                #     "data": results,
                #     "total_count": search_result["hits"]["total"]["value"]
                # }

            r["duration"] = float('%.4f' % (time.time() - start_time))

        except Exception as e:
            return Response(str(e), status=400)

        return Response(
            json.dumps(r, indent=2, ensure_ascii=False),
            status=200,
            content_type="application/json; charset=utf-8",
            headers={"Access-Control-Allow-Origin": "*"}
        )
