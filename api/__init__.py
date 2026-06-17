from elasticsearch import Elasticsearch
from flask import Flask, Blueprint
from cli import load_config  # import load_config from your CLI

api_bp = Blueprint('api_bp', __name__)

def parse_es_doc_id(es_id: str) -> str:
    """
    Parse an Elasticsearch _id of the form:
    resource_id::passage_id

    Returns: resource_id&ref=passage_id
    """
    if "::" in es_id:
        resource_id, passage_id = es_id.split("::", 1)
        return f"{resource_id}&ref={passage_id}"
    else:
        return es_id

def create_app(config_name: str):
    """Create the Flask application using YAML configuration.

    :param config_name: alias of the config YAML (e.g., local, staging, prod)
    :type config_name: str
    :return: Flask app instance
    :rtype: Flask
    """
    app = Flask(__name__)

    # Load YAML config using your existing load_config function
    config_dict = load_config(config_name)
    app.config.update(config_dict)

    # Initialize Elasticsearch client if URL is present
    app.elasticsearch = Elasticsearch([app.config['ELASTICSEARCH_URL']]) if app.config.get('ELASTICSEARCH_URL') else None

    with app.app_context():
        # Import and register search endpoint
        from api.search import register_search_endpoint

        def compose_result(search_result):
            results = []
            for h in search_result['hits']['hits']:
                fields = h.get('_source', {}).copy()
                fields.pop("content", None)  # remove content if exists
                fields['dts_url'] = f"{app.config['DTS_URL']}/document?resource={parse_es_doc_id(h['_id'])}"
                results.append({
                    "id": h['_id'],
                    "score": h.get('_score'),
                    "fields": fields,
                    "highlight": h.get('highlight')
                })
            return results

        register_search_endpoint(api_bp, "1.0", compose_result)
        app.register_blueprint(api_bp)

    return app
