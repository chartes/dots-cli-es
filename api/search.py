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
    field1:prefix*,field2:"some phrase",field3:te?t

    Retourne une liste de clauses ES pour 'must', gère les wildcards
    et applique la bonne logique pour text vs keyword.
    """
    clauses = []
    if not query_param:
        return clauses

    for part in query_param.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue

        field, value = part.split(":", 1)
        field = field.strip()
        value = value.strip().strip('"')

        # Détecte les wildcards
        if "*" in value or "?" in value:
            # Si le champ est text (analyse), utiliser query_string
            clauses.append({
                "query_string": {
                    "query": f"{field}:{value}",
                    "default_operator": "AND"
                }
            })
        else:
            # Recherche exacte ou phrase
            clauses.append({
                "match_phrase_prefix": {
                    field: value
                }
            })

    return clauses

def register_search_endpoint(bp, api_version="1.0", compose_result_func: Callable[[str], list] = lambda s: []):
    def api_search_endpoint():
        start_time: float = time.time()
        # PARAMETERS
        index: str = request.args.get("index", None)
        query_param: str = request.args.get("query", None)

        # eg. range[year]=gte:1871,lte:1899
        ranges: list[dict] = parse_range_parameter()

        groupby_field: str = request.args.get("groupby[field]", None)
        groupby_after: str = request.args.get("groupby[after-page]", None)
        groupby_with_ids: int = request.args.get("groupby[with-ids]", 10000) or 10000

        no_highlight = request.args.get("no-highlight", False)
        no_highlight = type(no_highlight) == str

        filters_param = request.args.get("filters")

        #if query_param is None:
        #    return Response(status=400)

        # if request has pagination parameters
        # add links to the top-level object
        if 'page[number]' in request.args or 'page[size]' in request.args:
            num_page = int(request.args.get('page[number]', 1))
            page_size = min(
                int(current_app.config["SEARCH_RESULT_PER_PAGE"]) + num_page,
                int(request.args.get('page[size]'))
            )
        else:
            num_page = 1
            page_size = int(current_app.config["SEARCH_RESULT_PER_PAGE"])

        # Search, retrieve, filter, sort and paginate objs
        # eg. &sort=-year
        sort_criteriae: list[dict] = []
        if "sort" in request.args:
            sort_criteriae = []
            for criteria in request.args["sort"].split(','):
                if criteria.startswith('-'):
                    sort_order = "desc"
                    criteria = criteria[1:]
                else:
                    sort_order = "asc"
                # criteria = criteria.replace("-", "_")
                sort_criteriae.append({criteria: {"order": sort_order}})

        r = {}
        if hasattr(current_app, 'elasticsearch'):
            body_query = {
                "query": {
                    "bool": {
                        "must": [

                        ]
                    },
                },

                "aggregations": {

                },
                "highlight": {},
                "sort": [
                    #  {"creation": {"order": "desc"}}
                    *sort_criteriae
                ],
                "track_total_hits": True,
                "track_scores": True
            }
            if query_param:
                must_clauses = parse_query_param(query_param)
                if must_clauses:
                    body_query["query"]["bool"]["must"].extend(must_clauses)
            else:
                # Pas de query → match_all
                body_query["query"]["bool"]["must"].append({"match_all": {}})

            if query_param and not no_highlight:
                body_query["highlight"] = {
                    "type": "fvh",
                    "fields": {
                        "content": {}
                    },
                    "number_of_fragments": 100,
                    "options": {"return_offsets": False}
                }

            if ranges:
                body_query["query"]["bool"]['must'].extend([{"range": r} for r in ranges])

            if groupby_field is not None:
                body_query["aggregations"] = {
                    "items": {
                        "composite": {
                            "sources": [
                                {
                                    groupby_field: {
                                        "terms": {
                                            "field": groupby_field,
                                        },
                                    }
                                }
                            ],
                            "size": page_size,
                        }
                    },
                    "bucket_count": {
                        "cardinality": {
                            "field": groupby_field
                        },
                    },
                }
                body_query["size"] = 0

                sort_criteriae.reverse()
                #for crit in sort_criteriae:
                #    for crit_name, crit_order in crit.items():
                #        source = {
                #            crit_name: {
                #                "terms": {
                #                    "field": crit_name,
                #                    **crit_order},
                #            }
                #        }
                #        body["aggregations"]["items"]["composite"]["sources"].insert(0, source)

                if groupby_after is not None:
                    sources_keys = [list(s.keys())[0] for s in body_query["aggregations"]["items"]["composite"]["sources"]]
                    body_query["aggregations"]["items"]["composite"]["after"] = {key: value for key, value in
                                                                           zip(sources_keys, groupby_after.split(','))}
                    print(sources_keys, groupby_after,
                          {key: value for key, value in zip(sources_keys, groupby_after.split(','))})

            # if page_size is not None:
            # if groupby_field is not None:
            page: int = 0 if groupby_field is not None else num_page - 1

            body_query["from"]: int = page * page_size
            body_query["size"]: int = page_size
            # else:
            #    body["from"] = 0 * page_size
            #    body["size"] = page_size
            # print("WARNING: /!\ for debug purposes the query size is limited to", body["size"])

            if filters_param:
                es_filters = parse_filters_param(filters_param)
                if es_filters:
                    body_query["query"]["bool"].setdefault("filter", []).extend(es_filters)

            search_result_test = current_app.elasticsearch.search(index=current_app.config["DOCUMENT_INDEX"], body=body_query)
            print('search_result_test : \n\n')
            pprint.pprint(search_result_test)
            try:
                if index is None or len(index) == 0:
                    index = current_app.config["DOCUMENT_INDEX"]

                pprint.pprint(body_query)
                # perform the search
                search_result = current_app.elasticsearch.search(index=index, body=body_query)
                #pprint.pprint(search_result['aggregations'])
                results: list = compose_result_func(search_result)
                count: int = search_result['hits']['total']['value']

                # print(body, len(results), search['hits']['total'], index)
                pprint.pprint(search_result)
                if 'aggregations' in search_result:
                    after_key = None
                    buckets = search_result["aggregations"]["items"]["buckets"]

                    # grab the after_key returned by ES for future queries
                    if "after_key" in search_result["aggregations"]["items"]:
                        after_key = search_result["aggregations"]["items"]["after_key"]
                    print("aggregations: {0} buckets; after_key: {1}".format(len(buckets), after_key))
                    # pprint.pprint(buckets)
                    bucket_count = search_result["aggregations"]["bucket_count"]["value"]

                    if groupby_with_ids is not False and len(buckets) > 0:
                        try:
                            size_limit = int(groupby_with_ids)
                        except Exception as e:
                            print(str(e))
                            print("Size limit reached in aggregation ids fetch:", groupby_with_ids)
                            size_limit = 10000

                        for bucket in buckets:
                            ids_query = {
                                "query": {
                                    "query_string": {
                                        "query": f"({query}) AND {groupby_field}:{bucket['key'][groupby_field]}"
                                    }
                                },
                                "size": size_limit
                            }
                            ids_result = current_app.elasticsearch.search(index=index, body=ids_query)
                            bucket['_ids'] = sorted([h["_id"] for h in ids_result['hits']["hits"]])

                    r = {
                        #"data": results,
                        "buckets": buckets,
                        "after_key": after_key,
                        "total-count": count,
                        "bucket-count": bucket_count
                    }
                else:
                    r = {
                        "data": results,
                        "total-count": count
                    }

                r["duration"] = float('%.4f' % (time.time() - start_time))

            except Exception as e:
                print("ES response error : ", str(e))
                return Response(str(e), status=400)

        return Response(
            json.dumps(r, indent=2, ensure_ascii=False),
            status=200,
            content_type="application/json; charset=utf-8",
            headers={
                "Access-Control-Allow-Origin": "*",
                # "Access-Control-Allow-Credentials": "true"
            }
        )

    # register the endpoint route
    bp.add_url_rule(f"/api/{api_version}/search", endpoint=api_search_endpoint.__name__, view_func=api_search_endpoint)
