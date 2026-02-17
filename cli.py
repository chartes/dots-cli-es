from csv import DictReader
import io
import json
import pprint
import re
from multiprocessing.resource_sharer import stop
import os
import csv
from datetime import datetime, timezone
import time
from typing import Any

import click
import requests

from api import create_app

clean_tags = re.compile('<.*?>')
body_tag = re.compile('<body(?:(?:.|\n)*?)>((?:.|\n)*?)</body>')

app = None

from lxml import etree

XML_NS = {"tei": "http://www.tei-c.org/ns/1.0"}


# timestamp pour ce run (UTC, timezone-aware)
ts = datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M")

def count_csv_rows(csv_path: str) -> int:
    """Compte le nombre de lignes (hors header) dans un CSV."""
    if not os.path.isfile(csv_path):
        return 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # 0 ou 1 ligne → uniquement le header
    return max(0, len(rows) - 1)

def format_duration(seconds: float) -> str:
    """Formate une durée en HH:MM:SS."""
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def report_timing(csv_path: str, row: dict):
    header = [
        "timestamp",
        "level",
        "id",
        "parent_id",
        "duration_sec",
        "duration_hms",
    ]
    ensure_csv_file(csv_path, header)

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow(row)

def ensure_csv_file(csv_path: str, header: list):
    """Crée le fichier CSV avec header si n'existe pas."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    if not os.path.isfile(csv_path):
        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()

def get_indexation_csv_paths(app):
    """
    Retourne les chemins des fichiers CSV horodatés pour CE run
    """
    base_dir = "indexation_reporting"
    os.makedirs(base_dir, exist_ok=True)

    return {
        "dts_sanitization": os.path.join(
            base_dir,
            f"{ts}_metadata_dts_sanitization.csv"
        ),
        "document_exceptions": os.path.join(
            base_dir,
            f"{ts}_{app.config['DOCUMENT_INDEX']}_indexation_exceptions.csv"
        ),
        "document_no_text": os.path.join(
            base_dir,
            f"{ts}_{app.config['DOCUMENT_INDEX']}_no_text.csv"
        ),
        "collection_exceptions": os.path.join(
            base_dir,
            f"{ts}_{app.config['COLLECTION_INDEX']}_indexation_exceptions.csv"
        ),
        "timing": os.path.join(
            base_dir,
            f"{ts}_dots_indexation_timing.csv"
        ),
        "indexed_passages_report": os.path.join(
            base_dir,
            f"{ts}_indexed_passages_report.csv"
        )
    }

def sanitize_dts_json(data: dict, app, collection_id: str):
    """
    Nettoie récursivement le JSON DTS :
    - supprime les clés vides
    - nettoie dict ET list
    - reporte les anomalies
    """

    anomalies = []

    def clean(value, path="root"):
        # 🔁 Cas dictionnaire
        if isinstance(value, dict):
            cleaned = {}
            for k, v in value.items():
                current_path = f"{path}.{k}"

                # 🔴 Clé vide
                if not k or k.strip() == "":
                    anomalies.append({
                        "collection_id": collection_id,
                        "path": current_path,
                        "error": "empty_key",
                        "value": str(v)
                    })
                    continue

                cleaned_value = clean(v, current_path)
                cleaned[k] = cleaned_value

            return cleaned

        # 🔁 Cas liste
        elif isinstance(value, list):
            cleaned_list = []
            for idx, item in enumerate(value):
                cleaned_item = clean(item, f"{path}[{idx}]")
                cleaned_list.append(cleaned_item)
            return cleaned_list

        # Valeur simple
        else:
            return value

    cleaned_data = clean(data)

    if anomalies:
        report_dts_sanitization(app, collection_id, anomalies)
    #print('cleaned data ', cleaned_data)
    return cleaned_data

def report_indexation_event(csv_path: str, row: dict, header: list):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow(row)

def report_passages_indexation(app, resource_id: str, passage_id: str):
    _csv_path = get_indexation_csv_paths(app)["indexed_passages_report"]
    report_indexation_event(
        csv_path=_csv_path,
        header=[
            "timestamp",
            "resource_id",
            "passage_id"
        ],
        row={
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resource_id": resource_id,
            "passage_id": passage_id
        }
    )

def report_indexation_exception(app, resource_id: str, passage_id: str, error: Exception, context: str):
    _csv_path = get_indexation_csv_paths(app)["document_exceptions"]
    report_indexation_event(
        csv_path=_csv_path,
        header=[
            "timestamp",
            "resource_id",
            "passage_id",
            "error_type",
            "error_message",
            "context"
        ],
        row={
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resource_id": resource_id,
            "passage_id": passage_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context
        }
    )

def report_no_text_passage(app, resource_id: str, passage_id: str, nav: dict):
    _csv_path = get_indexation_csv_paths(app)["document_no_text"]
    report_indexation_event(
        csv_path=_csv_path,
        header=[
            "timestamp",
            "resource_id",
            "passage_id",
            "citeType",
            "level",
            "reason",
            "context"
        ],
        row={
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resource_id": resource_id,
            "passage_id": passage_id,
            "citeType": nav.get("citeType"),
            "level": nav.get("level"),
            "reason": "NoIndexableText",
            "context": "index_resource_passages"
        }
    )

def report_collection_exception(app, collection_id: str, error: Exception, context: str):
    _csv_path = get_indexation_csv_paths(app)["collection_exceptions"]
    report_indexation_event(
        csv_path=_csv_path,
        header=["timestamp", "collection_id", "error_type", "error_message", "context"],
        row={
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "collection_id": collection_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context
        }
    )

def report_dts_sanitization(app, collection_id: str, anomalies: list[dict]):
    _csv_path = get_indexation_csv_paths(app)["dts_sanitization"]

    header = [
        "timestamp",
        "collection_id",
        "json_path",
        "error_type",
        "value"
    ]

    for anomaly in anomalies:
        report_indexation_event(
            csv_path=_csv_path,
            header=header,
            row={
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "collection_id": collection_id,
                "json_path": anomaly.get("path"),
                "error_type": anomaly.get("error"),
                "value": anomaly.get("value"),
            }
        )

def load_excluded_collections_from_settings(settings_path: str) -> set:
    """
    Parcourt les *.conf.json d'un dossier et extrait excludeCollectionIds
    """
    excluded = set()

    if not settings_path or not os.path.isdir(settings_path):
        return excluded

    for filename in os.listdir(settings_path):
        if not filename.endswith(".conf.json"):
            continue

        filepath = os.path.join(settings_path, filename)

        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)

            for coll_id in data.get("excludeCollectionIds", []):
                if coll_id:
                    excluded.add(coll_id.lower())

        except Exception as e:
            print(f"⚠️ Impossible de lire {filepath}: {e}")

    return excluded

def extract_passage_text(element, nav_index=None) -> str:
    """
    Extrait le texte d'un élément TEI dans l'ordre naturel de lecture.
    - Exclut uniquement les descendants avec xml:id dans nav_index.
    - Inclut tout texte direct et variantes (<sic>, <corr>, <lem>, <rdg>, etc.).
    """
    if nav_index is None:
        nav_index = {}

    def _recurse(el):
        texts = []

        xmlid = el.get("{http://www.w3.org/XML/1998/namespace}id")
        # ignorer uniquement descendants dans nav_index
        if el != element and xmlid in nav_index:
            return texts

        # texte avant enfants
        if el.text:
            texts.append(el.text)

        # parcourir enfants
        for child in el:
            texts.extend(_recurse(child))
            # texte après enfant (tail)
            if child.tail:
                texts.append(child.tail)

        return texts

    all_texts = _recurse(element)
    # nettoyage : retirer espaces, tabs, newlines inutiles
    cleaned_texts = [t.strip() for t in all_texts if t.strip()]
    return " ".join(cleaned_texts)


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

def normalize_metadata_value(value):
    """
    Normalise une valeur DTS (dc / extensions) pour ES :
    - dict → @id / label
    - list → liste de valeurs normalisées
    - scalar → string
    """
    if value is None:
        return None

    if isinstance(value, dict):
        return (
            value.get("@id")
            or value.get("id")
            or value.get("label")
            or str(value)
        )

    if isinstance(value, list):
        values = []
        for v in value:
            nv = normalize_metadata_value(v)
            if nv is not None:
                values.append(nv)
        return values

    # int, float, str, bool…
    return str(value)

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

def build_navigation_index(app, dts_url: str, resource_id: str) -> dict[Any, Any] | None:
    data = None
    for attempt in range(3):
        try:
            response = requests.get(
                f"{dts_url}/navigation",
                params={"resource": resource_id, "down": -1}
            )
            response.raise_for_status()
            data = response.json()
            #print('build_navigation_index' , dts_url, resource_id, response)
            break

        except requests.exceptions.HTTPError as e:
            if attempt == 2:
                # log proprement l'erreur HTTP
                report_indexation_event(
                    csv_path=get_indexation_csv_paths(app)["document_exceptions"],
                    header=[
                        "timestamp",
                        "resource_id",
                        "passage_id",
                        "error_type",
                        "error_message",
                        "context"
                    ],
                    row={
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "resource_id": resource_id,
                        "passage_id": '',
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "context": "dts_navigation"
                    }
                )
                return None  # 🔥 on skip
            time.sleep(2)

        except Exception as e:
            # log toute autre erreur réseau / JSON
            report_indexation_event(
                csv_path=get_indexation_csv_paths(app)["document_exceptions"],
                header=[
                    "timestamp",
                    "resource_id",
                    "passage_id",
                    "error_type",
                    "error_message",
                    "context"
                ],
                row={
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "resource_id": resource_id,
                    "passage_id": '',
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "context": "dts_navigation_unexpected"
                }
            )
            return None

    if data is None:
        return None

    nav = {}
    for item in data.get("member", []):
        # utilisez 'identifier' au lieu de 'id'
        passage_id = item.get("identifier")
        if not passage_id:
            continue  # ignore les items sans identifiant

        nav[passage_id] = {
            "id": passage_id,
            "citeType": item.get("citeType"),
            "level": item.get("level"),
            "parent": item.get("parent")
        }
    #print('build_navigation_index result : ', nav)
    return nav


def extract_resource_metadata(resource_meta_response: dict) -> dict:
    """
    Extrait les métadonnées DTS d'une Resource depuis /collection?id=RESOURCE_ID
    """
    metadata = {}

    # ─────────────────────────────
    # Standard DTS fields
    # ─────────────────────────────

    if "description" in resource_meta_response:
        metadata["description"] = resource_meta_response.get("description")

    if "@id" in resource_meta_response:
        metadata["id"] = resource_meta_response.get("@id")

    # ─────────────────────────────
    # Dublin Core (flat structure)
    # ─────────────────────────────

    dc = resource_meta_response.get("dublinCore", {})
    if isinstance(dc, dict):
        for key, value in dc.items():
            if value is None:
                continue
            metadata[key] = value

    # ─────────────────────────────
    # Extensions DTS (normalised)
    # ─────────────────────────────

    ext = resource_meta_response.get("extensions", {})
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
    #print('extract metadata \n')
    #pprint.pprint(response)
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

        "download": response.get("download")
    }

    # Add Dublin Core:
    dublincore = {}
    dc = response.get("dublincore", {})

    if isinstance(dc, dict):
        for key, value in dc.items():
            normalized = normalize_metadata_value(value)
            if normalized is None:
                continue
            dublincore[key] = normalized

    metadata["dublincore"] = dublincore
    # if isinstance(dc, dict):
    #     for key, value in dc.items():
    #         if value is None:
    #             continue
    #
    #         # transformer en string si c'est un dict ou une liste
    #         if isinstance(value, dict):
    #             # par exemple prendre uniquement le label/id si existant
    #             value = value.get("label") or value.get("@id") or str(value)
    #         elif isinstance(value, list):
    #             normalized = []
    #             for v in value:
    #                 if isinstance(v, dict):
    #                     normalized.append(
    #                         v.get("label") or v.get("@id") or str(v)
    #                     )
    #                 else:
    #                     normalized.append(str(v))
    #             value = ", ".join(normalized)
    #         elif not isinstance(value, str):
    #             value = str(value)
    #         dublincore[key] = value
    # metadata["dublincore"] = dublincore

    # Add extensions:
    extensions = {}

    ext = response.get("extensions", {})
    if isinstance(ext, dict):
        for key, value in ext.items():
            normalized = normalize_metadata_value(value)
            if normalized is None:
                continue
            extensions[key] = normalized

    metadata["extensions"] = extensions
    # extensions = {}
    # ext = response.get("extensions", {})
    # if isinstance(dc, dict):
    #     for key, value in ext.items():
    #         if value is None:
    #             continue
    #
    #         # transformer en string si c'est un dict ou une liste
    #         if isinstance(value, dict):
    #             # par exemple prendre uniquement le label/id si existant
    #             value = value.get("label") or value.get("@id") or str(value)
    #         elif isinstance(value, list):
    #             normalized = []
    #             for v in value:
    #                 if isinstance(v, dict):
    #                     normalized.append(
    #                         v.get("label") or v.get("@id") or str(v)
    #                     )
    #                 else:
    #                     normalized.append(str(v))
    #             value = ", ".join(normalized)
    #         elif not isinstance(value, str):
    #             value = str(value)
    #         extensions[key] = value
    # metadata["extensions"] = extensions

    # Add members:
    members = {}
    mbers = response.get("member", [])
    if isinstance(mbers, dict):
        for key, value in dc.items():
            if value is None:
                continue

            # transform to a string if this is a dict or a list
            if isinstance(value, dict):
                # for example, only use label/id if they exist
                value = value.get("label") or value.get("@id") or str(value)
            elif isinstance(value, list):
                normalized = []
                for v in value:
                    if isinstance(v, dict):
                        normalized.append(
                            v.get("label") or v.get("@id") or str(v)
                        )
                    else:
                        normalized.append(str(v))
                value = ", ".join(normalized)
            elif not isinstance(value, str):
                value = str(value)
            members[key] = value
    metadata["members"] = members

    return metadata

def index_resource_passages(
    app,
    resource_id: str,
    collection_metadata: dict,
    resource_metadata: dict
):
    dts_url = app.config["DTS_URL"]
    print('index_resource_passages', resource_id)
    nav_index = build_navigation_index(app, dts_url, resource_id)

    if nav_index is None:
        return  # there was an error getting the navigation : we skip the document and go to the next one

    xml_response = requests.get(
        f"{dts_url}/document",
        params={"resource": resource_id}
    )
    xml_response.raise_for_status()

    root = etree.fromstring(xml_response.content)

    # initialise ES bulk actions for all the resource fragments
    bulk_actions = []

    # FALLBACK : no DTS fragments for the resource → index all <text>
    if not nav_index:
        try:
            text_nodes = root.xpath("//tei:text", namespaces=XML_NS)
            if not text_nodes:
                raise ValueError("No <tei:text> found in TEI document")

            full_text = normalize_text(
                extract_passage_text(text_nodes[0], nav_index={})
            )

            if not full_text:
                raise ValueError("Empty fulltext extracted")

            document_passage = {
                "resource_id": resource_id,
                "passage_id": "__fulltext__",
                "citeType": "text",
                "level": 1,
                "content": full_text,
                "path": collection_metadata["path"],
                "path_ids": collection_metadata["path_ids"],
                "ancestors": [],
                "collection_metadata": {
                    "collection_id": collection_metadata["id"],
                    "collection_title": collection_metadata["title"],
                    "path": collection_metadata["path"],
                    "path_ids": collection_metadata["path_ids"],
                    "level": collection_metadata["level"],
                    "dublinCore": collection_metadata.get("dublinCore", {}),
                },
                "resource_metadata": resource_metadata
            }

            bulk_actions.append({
                "index": {
                    "_index": app.config["DOCUMENT_INDEX"],
                    "_id": f"{resource_id}::__fulltext__"
                }
            })
            bulk_actions.append(document_passage)

            # Passages counter
            app.index_stats["passages"] += 1

            report_passages_indexation(app, resource_id, '__fulltext__')

        except Exception as e:
            report_indexation_exception(
                app=app,
                resource_id=resource_id,
                passage_id="__fulltext__",
                error=e,
                context="index_resource_passages_fulltext_fallback"
            )

        # 👉 IMPORTANT : in this case, the main loop is skipped
        nav_index = {}


    for passage_id in nav_index:
        try:
            results = root.xpath(
                f"//*[@xml:id='{passage_id}']",
                namespaces=XML_NS
            )

            if not results:
                raise IndexError("xml:id not found in TEI document")

            el = results[0]

        except Exception as e:
            report_indexation_exception(
                app=app,
                resource_id=resource_id,
                passage_id=passage_id,
                error=e,
                context="index_resource_passages"
            )
            print(
                f"⚠️ Passage {passage_id} absent du TEI "
                f"(resource {resource_id}) → loggé"
            )
            continue

        if not isinstance(el, etree._Element):
            print("⚠️ Skipping non-element", el)
            continue

        text = normalize_text(extract_passage_text(el, nav_index=nav_index))

        if not text:
            report_no_text_passage(
                app=app,
                resource_id=resource_id,
                passage_id=passage_id,
                nav=nav_index.get(passage_id, {})
            )
            continue

        nav = nav_index.get(passage_id, {})
        ancestors = get_ancestors(passage_id, nav_index)

        document_passages = {
            "resource_id": resource_id,
            "passage_id": passage_id,
            "citeType": nav.get("citeType"),
            "level": nav.get("level"),
            "content": text,
            "path": collection_metadata["path"],
            "path_ids": collection_metadata["path_ids"],
            "ancestors": ancestors,
            "collection_metadata": {
                "collection_id": collection_metadata["id"],
                "collection_title": collection_metadata["title"],
                "path": collection_metadata["path"],
                "path_ids": collection_metadata["path_ids"],
                "level": collection_metadata["level"],
                "dublinCore": collection_metadata.get("dublinCore", {}),
            },
            "resource_metadata": resource_metadata
        }
        #print('working till here ', resource_id, passage_id)

        # Add fragment to ES bulk actions
        bulk_actions.append({
            "index": {
                "_index": app.config["DOCUMENT_INDEX"],
                "_id": f"{resource_id}::{passage_id}"
            }
        })
        bulk_actions.append(document_passages)

        # Fragments counter
        app.index_stats["passages"] += 1

        report_passages_indexation(app, resource_id, passage_id)

    # Bulk load all fragments
    # pprint.pprint(bulk_actions)
    if bulk_actions:
        try:
            response = app.elasticsearch.bulk(
                body=bulk_actions,
                refresh=False
            )
        except Exception as e:
            report_indexation_exception(
                app=app,
                resource_id=resource_id,
                passage_id="*",
                error=e,
                context="bulk_index_resource_passages"
            )
        else:
            # Trace ES errors at passage level
            if response.get("errors"):
                for item in response.get("items", []):
                    action = item.get("index", {})
                    if "error" in action:
                        es_id = action.get("_id", "")
                        passage_id = (
                            es_id.split("::", 1)[1]
                            if "::" in es_id else es_id
                        )
                        report_indexation_exception(
                            app=app,
                            resource_id=resource_id,
                            passage_id=passage_id,
                            error=Exception(action["error"].get("reason", "ES bulk error")),
                            context="bulk_index_resource_passages_es_error"
                        )
                app.index_stats["bulk_es_errors"] += 1

    # Resource indexation
    document = {
        "type": "Resource",
        "level": 0,
        "resource_metadata": resource_metadata
    }
    try:
        app.elasticsearch.index(
            index=app.config["DOCUMENT_INDEX"],
            id=f"{resource_id}",
            body=document
        )
        # Resources counter (DTS: resources of @type Resource)
        app.index_stats["resources"] += 1

    except Exception as e:
        report_indexation_exception(
            app=app,
            resource_id=resource_id,
            passage_id="*",
            error=e,
            context="index_resource_metadata"
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

    # 1️⃣ Fetching resource content from DTS
    response = requests.get(f'{_DTS_URL}/document', params={"resource": resource_id})
    response.raise_for_status()

    content = extract_body(response.text)
    content = remove_html_tags(content)

    # 2️⃣ Fetching resource metadata from DTS
    meta_response = requests.get(
        f'{_DTS_URL}/collection',
        params={"id": resource_id}
    )
    meta_response.raise_for_status()

    resource_meta_response = meta_response.json()
    resource_metadata = extract_resource_metadata(resource_meta_response)

    # 3️⃣ Building ES record
    document = {
        "content": normalize_text(content),

        # DTS metadata (remplacing former TSV)
        "metadata": resource_metadata,

        # hierarchy
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
    target_collections: set = None,
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
    if collection_id.lower() in app.excluded_collections:
        print(f"⏭️  Collection exclue : {collection_id}")
        return

    _DTS_URL = app.config['DTS_URL']

    if visited is None:
        visited = set()

    # Avoid infinite loops within collections
    if collection_id in visited:
        return
    visited.add(collection_id)

    collection_start = time.perf_counter()

    # 1️⃣ Get collection details from DTS
    try:
        response = requests.get(f"{_DTS_URL}/collection?id={collection_id}")
        response.raise_for_status()
        #data = response.json()
        data = sanitize_dts_json(response.json(), app, collection_id)
        #print('data test', data)
    except Exception as e:
        print(f"⚠️ Impossible de récupérer la collection {collection_id}: {e}")
        report_collection_exception(app, collection_id, e, context="crawl_collection_dts_response")
        return  # Do no attenmpt to crawl the collection in this case

    # Ignore if not a collection
    if data.get("@type") != "Collection":
        return

    # 2️⃣ Extract collection metadata
    collection_metadata = extract_metadata(
        data,
        parent_id=parent_id,
        parent_path=parent_path,
        parent_path_ids=parent_path_ids
    )

    #print('/n/n crawl_collection collection_id', collection_id)
    #print('/n/n crawl_collection data', data)
    #print('/n/n crawl_collection collection_metadata', collection_metadata)
    # 3️⃣ Normalized collection id for ES
    collection_es_id = collection_metadata.get("id") or f"collection_{collection_id}"

    # Check if collection should be included
    collection_id_lc = collection_id.lower()

    index_current = (
            target_collections is None
            or collection_id_lc in target_collections
            or (parent_id and parent_id.lower() in target_collections)
    )
    #print('\n\n TEST index_current ', index_current, collection_id.lower(), target_collections)

    # 4️⃣ Collection indexation (if collection in allowed scope)
    if index_current:
        try:
            # Projects and collections counter (only effectively indexed ones)
            if collection_metadata.get("level") == 1:
                app.index_stats["projects"] += 1
            elif collection_metadata.get("level") > 1:
                app.index_stats["collections"] += 1

            app.elasticsearch.index(
                index=collection_index,
                id=collection_es_id,
                body=collection_metadata
            )
            print(f"Indexed collection {collection_metadata.get('path', collection_es_id)}")
        except Exception as e:
            print(f"Impossible d’indexer la collection {collection_es_id}: {e}")
            report_collection_exception(app, collection_id, e, context="crawl_collection_indexation")
            return

    # 5️⃣ Parcours des membres de la collection
    for member in data.get("member", []):
        member_type = member.get("@type")
        member_id = member.get("@id")

        if not member_id:
            # Ignore les membres sans @id
            continue

        if member_type == "Collection":
            # Appel récursif pour sous-collections
            crawl_collection(
                collection_id=member_id,
                collection_index=collection_index,
                target_collections=target_collections,
                visited=visited,
                parent_id=collection_es_id,
                parent_path=collection_metadata.get("path"),
                parent_path_ids=collection_metadata.get("path_ids")
            )

        elif member_type == "Resource":
            #print('crawl member resource ', member_id)
            #print('crawl member collection_metadata', collection_metadata)

            # indexer uniquement si parent ou descendant est ciblé
            #print('\n\n TEST index_current member_type == "Resource"', index_current)
            if index_current:
                resource_start = time.perf_counter()

                resource_metadata = extract_metadata(
                    member,
                    parent_id=collection_es_id,
                    parent_path=collection_metadata.get("path"),
                    parent_path_ids=collection_metadata.get("path_ids")
                )

                # Indexation du Resource DTS au niveau des passages
                index_resource_passages(
                    app=app,
                    resource_id=member_id,
                    collection_metadata=collection_metadata,
                    resource_metadata=resource_metadata
                )

                resource_duration = time.perf_counter() - resource_start

                report_timing(
                    get_indexation_csv_paths(app)["timing"],
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "level": "resource",
                        "id": member_id,
                        "parent_id": collection_id,
                        "duration_sec": round(resource_duration, 3),
                        "duration_hms": format_duration(resource_duration),
                    }
                )
    collection_duration = time.perf_counter() - collection_start

    report_timing(
        get_indexation_csv_paths(app)["timing"],
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "collection",
            "id": collection_id,
            "parent_id": parent_id or "",
            "duration_sec": round(collection_duration, 3),
            "duration_hms": format_duration(collection_duration),
        }
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
    @click.option("--collections", "-c", default=None, help="Comma separated collection ids to index, ex: coll1, coll2,coll3")
    def index(years, collections):
        """
        Rebuild the elasticsearch indexes
        Optionally, limit indexing to specific collections with --collections.
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
            print("Crawling DTS collections and resources from root_collection_id: ", root_collection_id)

            # collection exclusion
            settings_excluded = load_excluded_collections_from_settings(
                app.config.get("CUSTOM_SETTINGS_PATH")
            )
            print("A custom setting folder was defined: ", app.config.get("CUSTOM_SETTINGS_PATH"))
            print("These settings will exclude the following collections ids: ", settings_excluded)

            manual_excluded = set(app.config.get("ADDITIONAL_EXCLUDED_COLLECTIONS", []))
            print("You have manually also excluded the following collections ids:", manual_excluded)

            excluded_collections = settings_excluded | manual_excluded

            app.excluded_collections = excluded_collections

            # statistiques partagées du run d'indexation
            app.index_stats = {
                "projects": 0,  # collections sans parent
                "collections": 0,  # collections avec parent
                "resources": 0,
                "passages": 0,
                "bulk_es_errors": 0
            }

            start_time = time.perf_counter()

            print("Crawling DTS collections and resources from root_collection_id …", root_collection_id)
            # fichiers horodatés (calculés après que app soit créé)
            csv_paths = get_indexation_csv_paths(app)

            # création des fichiers de reporting vides avec header
            ensure_csv_file(csv_paths["document_exceptions"], [
                "timestamp", "resource_id", "passage_id", "error_type", "error_message", "context"
            ])
            ensure_csv_file(csv_paths["document_no_text"], [
                "timestamp", "resource_id", "passage_id", "citeType", "level", "reason", "context"
            ])
            ensure_csv_file(csv_paths["collection_exceptions"], [
                "timestamp", "collection_id", "error_type", "error_message", "context"
            ])
            ensure_csv_file(csv_paths["indexed_passages_report"], [
                "timestamp", "resource_id", "passage_id"
            ])

            # --- traitement des collections ciblées ---
            if collections:
                # split et suppression des espaces autour
                target_collections = {c.strip().lower() for c in collections.split(",") if c.strip()}
                print(f"Collections ciblées pour l'indexation: {target_collections}")
            else:
                target_collections = None  # tout indexer

            crawl_collection(
                collection_id=root_collection_id,
                collection_index=_index_name,
                target_collections=target_collections
            )
            # ─────────────────────────────
            # Résumé de fin d’indexation
            # ─────────────────────────────

            doc_errors = count_csv_rows(csv_paths["document_exceptions"])
            doc_no_text = count_csv_rows(csv_paths["document_no_text"])
            collection_errors = count_csv_rows(csv_paths["collection_exceptions"])

            end_time = time.perf_counter()
            duration = format_duration(end_time - start_time)

            stats = app.index_stats

            target = app.config.get("TARGET_COLLECTION")

            if target:
                title = f"📊  Résumé de l’indexation de la collection cible {target}"
            else:
                title = "📊  Résumé de l’indexation"

            print("\n" + "=" * 60)
            print(title)
            print("=" * 60)
            print(f"🗂️  Projets (collections racine) : {stats['projects']}")
            print(f"📁  Sous-collections            : {stats['collections']}")
            print(f"📄  Resources indexées          : {stats['resources']}")
            print(f"🧩  Passages indexés            : {stats['passages']}")
            print("────────────────────────")
            print(f"📄  Passages en erreur        : {doc_errors}")
            print(f"📄  Passages sans texte       : {doc_no_text}")
            print(f"🗂️  Collections en erreur     : {collection_errors}")
            print(f"❌  Erreurs ES (bulk)           : {stats['bulk_es_errors']}")
            print("────────────────────────")
            print(f"🕒  Timestamp du run          : {ts}")
            print(f"⏱️  Durée totale du run       : {duration}")
            print("=" * 60 + "\n")
            #print("DTS collections and documents indexed successfully.")

        except Exception as e:
            print('Indexation error (collections): ', str(e))

    cli.add_command(delete_indexes)
    cli.add_command(update_conf)
    cli.add_command(index)
    cli.add_command(search)
    return cli
