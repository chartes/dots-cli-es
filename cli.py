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

from lxml import etree

XML_NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def extract_passage_text(element) -> str:
    """
    Extrait le texte d'un élément TEI pour l'indexation.
    - Si l'élément a des descendants avec @xml:id, on exclut leur texte.
    - Sinon on prend tout le texte.
    """
    # cherche descendants avec xml:id
    descendants_with_id = element.xpath(".//*[@xml:id]", namespaces=XML_NS)
    if descendants_with_id:
        # texte propre à cet élément, exclut descendants identifiés
        texts = element.xpath(
            "text() | ./node()[not(@xml:id)]//text()",
            namespaces=XML_NS
        )
    else:
        # pas de descendants identifiés → texte complet
        texts = element.xpath(".//text()", namespaces=XML_NS)

    return " ".join(t.strip() for t in texts if t.strip())


def remove_html_tags(text):
    return re.sub(clean_tags, ' ', text)

def normalize_text(text: str) -> str:
    """
    Nettoie le texte TEI :
    - supprime espaces / retours parasites
    - normalise les espaces
    """
    if not text:
        return ""

    # remplace tous les blancs (espaces, \n, \t) par un espace
    text = re.sub(r"\s+", " ", text)

    return text.strip()

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

def get_ancestors(passage_id: str, nav_index: dict) -> list:
    ancestors = []
    current = nav_index.get(passage_id)

    while current and current.get("parent"):
        parent_id = current["parent"]
        parent = nav_index.get(parent_id)
        if not parent:
            break
        ancestors.insert(0, {
            "id": parent["id"],
            "level": parent.get("level"),
            "citeType": parent.get("citeType")
        })
        current = parent

    return ancestors

def build_navigation_index(dts_url: str, resource_id: str) -> dict:
    response = requests.get(
        f"{dts_url}/navigation",
        params={"resource": resource_id, "down": -1}
    )
    response.raise_for_status()
    print('build_navigation_index' , dts_url, resource_id, response)
    nav = {}
    for item in response.json().get("member", []):
        # utilisez 'identifier' au lieu de 'id'
        passage_id = item.get("identifier")
        if not passage_id:
            continue  # ignore les items sans identifiant

        nav[passage_id] = {
            "id": passage_id,
            "citeType": item.get("citeType"),
            "level": item.get("level"),
            # parent doit aussi être normalisé si nécessaire
            "parent": item.get("parent") or item.get("parentIdentifier")
        }
    return nav


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
    print('extract_metadata id', )
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
        "totalParents": response.get("totalParents")
    }

    # Ajout de dublin Core:
    dublincore = {}
    dc = response.get("dublincore", {})
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
            dublincore[key] = value
    metadata["dublincore"] = dublincore

    # Ajout des members:
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

def index_resource_passages(
    app,
    resource_id: str,
    collection_metadata: dict
):
    dts_url = app.config["DTS_URL"]
    print('index_resource_passages', resource_id)
    nav_index = build_navigation_index(dts_url, resource_id)
    print('index_resource_passages nav_index', nav_index)

    xml_response = requests.get(
        f"{dts_url}/document",
        params={"resource": resource_id}
    )
    xml_response.raise_for_status()

    root = etree.fromstring(xml_response.content)
    print('index_resource_passages', root)
    for el in root.xpath("//*[@xml:id]", namespaces=XML_NS):
        print("⚠️ Checking el", el)
        passage_id = el.get("{http://www.w3.org/XML/1998/namespace}id")

        # ⚡ NE PRENDRE QUE LES PASSAGES DANS LA NAVIGATION
        if passage_id not in nav_index:
            continue

        if not isinstance(el, etree._Element):
            print("⚠️ Skipping non-element", el)
            continue
        text = extract_passage_text(el)
        print('index_resource_passages text', text)

        if not text:
            continue

        nav = nav_index.get(passage_id, {})
        print('index_resource_passages', nav)
        ancestors = get_ancestors(passage_id, nav_index)
        print('index_resource_passages', ancestors)

        document = {
            "resource_id": resource_id,
            "passage_id": passage_id,
            "citeType": nav.get("citeType"),
            "level": nav.get("level"),
            "content": text,
            "path": collection_metadata["path"],
            "path_ids": collection_metadata["path_ids"],
            "ancestors": ancestors,
            "metadata": {
                "collection_id": collection_metadata["id"],
                "collection_title": collection_metadata["title"],
                "path": collection_metadata["path"],
                "path_ids": collection_metadata["path_ids"],
                "level": collection_metadata["level"],
                "dublinCore": collection_metadata.get("dublinCore", {}),
            }
        }

        app.elasticsearch.index(
            index=app.config["DOCUMENT_INDEX"],
            id=f"{resource_id}::{passage_id}",
            body=document
        )


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
        "content": normalize_text(content),

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

def crawl_collection(
    collection_id: str,
    collection_index: str,
    visited=None,
    parent_id=None,
    parent_path=None,
    parent_path_ids=None
):
    """
    Crawl recursively a DTS collection and index:
    - the collection itself in COLLECTION_INDEX
    - all Resources as passages in DOCUMENT_INDEX
    """

    _DTS_URL = app.config['DTS_URL']

    if visited is None:
        visited = set()

    # éviter les boucles infinies
    if collection_id in visited:
        return
    visited.add(collection_id)

    # 1️⃣ Récupération de la collection depuis DTS
    response = requests.get(f"{_DTS_URL}/collection?id={collection_id}")
    response.raise_for_status()
    data = response.json()

    # Ignore si ce n’est pas une collection
    if data.get("@type") != "Collection":
        return

    # 2️⃣ Extraction des métadonnées de la collection
    metadata = extract_metadata(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        parent_path_ids=parent_path_ids
    )
    print('/n/n crawl_collection collection_id', collection_id)
    #print('/n/n crawl_collection data', data)
    print('/n/n crawl_collection metadata', metadata)
    # 3️⃣ Création d'un ID sûr pour Elasticsearch
    collection_es_id = metadata.get("id") or f"collection_{collection_id}"

    # 4️⃣ Indexation de la collection
    try:
        app.elasticsearch.index(
            index=collection_index,
            id=collection_es_id,
            body=metadata
        )
        print(f"Indexed collection {metadata.get('path', collection_es_id)}")
    except Exception as e:
        print(f"Impossible d’indexer la collection {collection_es_id}: {e}")
        return

    # 5️⃣ Parcours des membres de la collection
    for member in data.get("member", []):
        member_type = member.get("@type")
        member_id = member.get("@id")

        if not member_id:
            # Ignore les membres sans @id
            continue

        if member_type == "Collection" and member_id != 'cartulaires':
            # Appel récursif pour sous-collections
            crawl_collection(
                collection_id=member_id,
                collection_index=collection_index,
                visited=visited,
                parent_id=collection_es_id,
                parent_path=metadata.get("path"),
                parent_path_ids=metadata.get("path_ids")
            )

        elif member_type == "Resource":
            print('crawl member resource ', member_id)
            print('crawl member resource metadata', metadata)
            # Indexation du Resource DTS au niveau des passages
            index_resource_passages(
                app=app,
                resource_id=member_id,
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
