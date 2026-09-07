"""Compatibility shim for deployments referencing `flask_app:flask_app` (e.g. uWSGI)."""
from dots_es.flask_app import flask_app, main

__all__ = ["flask_app", "main"]

if __name__ == "__main__":
    main()
