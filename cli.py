from csv import DictReader
import io
import json
import pprint
import re
from multiprocessing.resource_sharer import stop

import click
import requests

from api import create_app

clean_tags = re.compile('<.*?>')
body_tag = re.compile('<body(?:(?:.|\n)*?)>((?:.|\n)*?)</body>')

app = None


def remove_html_tags(text):
    return re.sub(clean_tags, ' ', text)


def extract_body(text):
    match = re.search(body_tag, text)
    if match:
        return match.group(1)
    return text


def load_elastic_conf(index_name, rebuild=False):
    url = '/'.join([app.config['ELASTICSEARCH_URL'], index_name])
    res = None
    try:
        if rebuild:
            print(f"Deleting {index_name} index.")
            res = requests.delete(url)
        with open('elasticsearch/_global.conf.json', 'r') as _global:
            global_settings = json.load(_global)

            with open(f'elasticsearch/{index_name}.conf.json', 'r') as f:
                payload = json.load(f)
                payload["settings"] = global_settings
                print("UPDATE INDEX CONFIGURATION:", url)
                res = requests.put(url, json=payload)
                assert str(res.status_code).startswith("20")

    except FileNotFoundError as e:
        print(str(e))
        print("conf not found", flush=True, end=" ")
    except Exception as e:
        print(res.text, str(e), flush=True, end=" ")
        raise e

def normalize_extension_key(key: str) -> str:
    """
    Normalize DTS extension keys for Elasticsearch:
    - replace ':' by '_'
    """
    return key.replace(":", "_")


def extract_resource_metadata(resource_collection_response: dict) -> dict:
    """
    Extrait les métadonnées DTS d'une Resource depuis /collection?id=RESOURCE_ID
    """
    metadata = {}

    # ─────────────────────────────
    # Champs DTS standards
    # ─────────────────────────────

    if "description" in resource_collection_response:
        metadata["description"] = resource_collection_response.get("description")

    if "@id" in resource_collection_response:
        metadata["id"] = resource_collection_response.get("@id")

    # ─────────────────────────────
    # Dublin Core (structure plate)
    # ─────────────────────────────

    dc = resource_collection_response.get("dublinCore", {})
    if isinstance(dc, dict):
        for key, value in dc.items():
            if value is None:
                continue
            metadata[key] = value

    # ─────────────────────────────
    # Extensions DTS (normalisées)
    # ─────────────────────────────

    ext = resource_collection_response.get("extensions", {})
    if isinstance(ext, dict):
        normalized_extensions = {}
        for key, value in ext.items():
            if value is None:
                continue
            normalized_key = normalize_extension_key(key)
            normalized_extensions[normalized_key] = value

        if normalized_extensions:
            metadata["extensions"] = normalized_extensions

    return metadata

def extract_metadata(response, parent_id=None, parent_path=None, parent_path_ids=None):
    title = response.get("title") or response.get("@id")

    path = title if not parent_path else f"{parent_path} > {title}"
    path_ids = [response.get("@id")] if not parent_path_ids else parent_path_ids + [response.get("@id")]



    metadata = {
        "id": response.get("@id"),
        "type": response.get("@type"),
        "title": title,
        "description": response.get("description"),

        "parent_id": parent_id,
        "path": path,
        "path_ids": path_ids,
        "level": len(path_ids) - 1,

        "dtsVersion": response.get("dtsVersion"),
        "totalItems": response.get("totalItems"),
        "totalChildren": response.get("totalChildren"),
        "totalParents": response.get("totalParents"),

        "members": response.get("member", [])
    }
    # Ajout de dublinCore:
    dublinCore = {}
    dc = response.get("dublinCore", {})
    if isinstance(dc, dict):
        for key, value in dc.items():
            if value is None:
                continue

            # transformer en string si c'est un dict ou une liste
            if isinstance(value, dict):
                # par exemple prendre uniquement le label/id si existant
                value = value.get("label") or value.get("@id") or str(value)
            elif isinstance(value, list):
                # transformer la liste en string (ou liste de strings)
                value = ", ".join(
                    str(v.get("label") if isinstance(v, dict) else v) for v in value
                )
            elif not isinstance(value, str):
                value = str(value)
            dublinCore[key] = value
    metadata["dublinCore"] = dublinCore

    # Ajout de dublinCore:
    members = {}
    mbers = response.get("member", [])
    if isinstance(mbers, dict):
        for key, value in dc.items():
            if value is None:
                continue

            # transformer en string si c'est un dict ou une liste
            if isinstance(value, dict):
                # par exemple prendre uniquement le label/id si existant
                value = value.get("label") or value.get("@id") or str(value)
            elif isinstance(value, list):
                # transformer la liste en string (ou liste de strings)
                value = ", ".join(
                    str(v.get("label") if isinstance(v, dict) else v) for v in value
                )
            elif not isinstance(value, str):
                value = str(value)
            members[key] = value
    metadata["members"] = members

    return metadata

def index_dts_resource(resource_id, collection_metadata):
    """
    Indexe une Resource DTS avec :
    - contenu texte
    - métadonnées DTS
    - héritage hiérarchique
    """

    _DTS_URL = app.config['DTS_URL']
    _index_name = app.config['DOCUMENT_INDEX']

    # 1️⃣ Récupération du contenu
    response = requests.get(f'{_DTS_URL}/document', params={"resource": resource_id})
    response.raise_for_status()

    content = extract_body(response.text)
    content = remove_html_tags(content)

    # 2️⃣ Récupération des métadonnées DTS
    meta_response = requests.get(
        f'{_DTS_URL}/collection',
        params={"id": resource_id}
    )
    meta_response.raise_for_status()

    resource_collection = meta_response.json()
    resource_metadata = extract_resource_metadata(resource_collection)

    # 3️⃣ Construction du document ES
    document = {
        "content": content,

        # métadonnées DTS (remplace le TSV)
        "metadata": resource_metadata,

        # hiérarchie
        "parent_collection_id": collection_metadata["id"],
        "path": f'{collection_metadata["path"]} > {resource_metadata.get("title", resource_id)}',
        "path_ids": collection_metadata["path_ids"] + [resource_id],
        "level": len(collection_metadata["path_ids"]),
        "collection_title": collection_metadata["title"]
    }

    app.elasticsearch.index(
        index=_index_name,
        id=resource_id,
        body=document
    )

    print(f"Indexed resource {resource_id}")

def crawl_collection(collection_id, collection_index, visited=None,
                     parent_id=None, parent_path=None, parent_path_ids=None):

    _DTS_URL = app.config['DTS_URL']

    if visited is None:
        visited = set()

    if collection_id in visited:
        return
    visited.add(collection_id)

    response = requests.get(f'{_DTS_URL}/collection?id={collection_id}')
    response.raise_for_status()
    data = response.json()

    if data.get("@type") != "Collection":
        return

    metadata = extract_metadata(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        parent_path_ids=parent_path_ids
    )

    # indexation collection
    app.elasticsearch.index(
        index=collection_index,
        id=metadata["id"],
        body=metadata
    )

    print(f"Indexed collection {metadata['path']}")

    # parcours des membres
    for member in data.get("member", []):
        if member.get("@type") == "Collection" and member.get("@id") != 'cartulaires':
            crawl_collection(
                collection_id=member.get("@id"),
                collection_index=collection_index,
                visited=visited,
                parent_id=metadata["id"],
                parent_path=metadata["path"],
                parent_path_ids=metadata["path_ids"]
            )

        elif member.get("@type") == "Resource":
            index_dts_resource(
                resource_id=member.get("@id"),
                collection_metadata=metadata
            )

def make_cli():
    """ Creates a Command Line Interface for everydays tasks

    :return: Click groum
    """

    @click.group()
    @click.option('--config', default="staging", type=click.Choice(["local", "staging", "prod"]), help="select appropriate .env file to use", show_default=True)
    def cli(config):
        global app
        app = create_app(config)
        app.all_indexes = f"{app.config['DOCUMENT_INDEX']},{app.config['COLLECTION_INDEX']}"

    @click.command("search")
    @click.argument('query')
    @click.option('--indexes', required=False, default=None, help="index names separated by a comma")
    @click.option('-t', '--term', is_flag=True, help="use a term instead of a whole query")
    def search(query, indexes, term):
        """
        Perform a search using the provided query. Use --term or -t to simply search a term.
        """
        if term:
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "query_string": {
                                    "query": query,
                                }
                            }
                        ]
                    },
                }
            }
        else:
            body = query

        config = {"index": indexes if indexes else app.all_indexes, "body": body}

        result = app.elasticsearch.search(**config)
        print("\n", "=" * 12, " RESULT ", "=" * 12)
        pprint.pprint(result)

    @click.command("update-conf")
    @click.option('--indexes', default=None, help="index names separated by a comma")
    @click.option('--rebuild', is_flag=True, help="truncate the index before updating its configuration")
    def update_conf(indexes, rebuild):
        """
        Update the index configuration and mappings
        """
        indexes = indexes if indexes else app.all_indexes
        for name in indexes.split(','):
            load_elastic_conf(name, rebuild=rebuild)

    @click.command("delete")
    @click.option('--indexes', required=True, help="index names separated by a comma")
    def delete_indexes(indexes):
        """
        Delete the indexes
        """
        indexes = indexes if indexes else app.all_indexes
        for name in indexes.split(','):
            url = '/'.join([app.config['ELASTICSEARCH_URL'], name])
            res = None
            try:
                print(f"Deleting {name} index.")
                res = requests.delete(url)
            except Exception as e:
                print(res.text, str(e), flush=True, end=" ")
                raise e

    @click.command("index")
    @click.option('--years', required=True, default="all", help="1987-1999")
    def index(years):
        """
        Rebuild the elasticsearch indexes
        """
        _index_name = app.config["DOCUMENT_INDEX"]
        if not app.elasticsearch.indices.exists(index=_index_name):
            print(f"Index {_index_name} not found.")
            load_elastic_conf(_index_name, rebuild=False)

        # _DTS_URL = app.config["DTS_URL"]
        # _target_collection = app.config["TARGET_COLLECTION"]
        # # BUILD THE METADATA DICT FROM THE GITHUB TSV FILE
        #
        # response = requests.get(f'{_DTS_URL}/collection?id={_target_collection}')
        # metadata = {}
        # print('response collection DTS URL', response.text)
        #
        # reader = DictReader(io.StringIO(response.text), delimiter="\t")
        # for row in reader:
        #     try:
        #         metadata[row["id"]] = {
        #             "author_name": row["author_name"],
        #             "author_firstname": row["author_firstname"],
        #             "title_rich": row["title_rich"],
        #             "promotion_year": int(row["promotion_year"]) if row["promotion_year"] else None,
        #             "topic_notBefore": int(row["topic_notBefore"]) if row["topic_notBefore"] else None,
        #             "topic_notAfter": int(row["topic_notAfter"]) if row["topic_notAfter"] else None,
        #             "author_gender": int(row["author_gender"]) if row["author_gender"] else None,
        #                 # 1/2, verify that there is no other value
        #             "author_is_enc_teacher": 1 if row["author_is_enc_teacher"]=="1" else None,
        #         }
        #     except Exception as exc:
        #         print(f"ERROR while indexing {row['id']}, {exc}")
        #
        # # INDEXATION DES DOCUMENTS
        # all_docs = []
        # try:
        #     if years == "all":
        #         years = app.config["ALL_YEARS"]
        #     start_year, end_year = (int(y) for y in years.split("-"))
        #     print("Fetching documents from DTS")
        #     for year in range(start_year, end_year + 1):
        #
        #         _ids = [
        #             d
        #             for d in metadata.keys()
        #             if str(year) in d and "_PREV" not in d and "_NEXT" not in d
        #         ]
        #
        #         for encpos_id in _ids:
        #             response = requests.get(f'{_DTS_URL}/document?resource={encpos_id}')
        #             print(encpos_id, response.status_code)
        #
        #             content = extract_body(response.text)
        #             content = remove_html_tags(content)
        #             all_docs.append("\n".join([
        #                 json.dumps(
        #                     {"index": {"_index": _index_name, "_id": encpos_id}}
        #                 ),
        #                 json.dumps(
        #                     {"content": content, "metadata": metadata[encpos_id]}
        #                 )
        #             ]))
        #
        #     print("Indexig documents in elasticsearch")
        #     app.elasticsearch.bulk(body=all_docs, request_timeout=60*10)
        #
        # except Exception as e:
        #     print('Indexation error: ', str(e))

        # INDEXATION DES COLLECTIONS (DTS)
        try:
            _index_name = app.config['COLLECTION_INDEX']

            # collection racine DTS
            root_collection_id = app.config['TARGET_COLLECTION']

            print("Crawling DTS collections and resources from root_collection_id …", root_collection_id)

            crawl_collection(
                collection_id=root_collection_id,
                collection_index=_index_name
            )

            print("DTS collections and documents indexed successfully.")

        except Exception as e:
            print('Indexation error (collections): ', str(e))

    cli.add_command(delete_indexes)
    cli.add_command(update_conf)
    cli.add_command(index)
    cli.add_command(search)
    return cli
