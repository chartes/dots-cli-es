# Installation

## 1. Elasticsearch and the ICU plugin

The `folding` analyzer declared in `_global.conf.json` uses `icu_folding`, so **the `analysis-icu`
plugin is mandatory** — without it, index creation fails.

!!! warning
    Use an Elasticsearch version compatible with `requirements.txt` (8.12+). Run the commands below
    *outside* your virtual environment (`deactivate` first).

=== "Existing installation"

    Check whether ICU is already available:

    ```bash
    uconv -V
    ```

    Otherwise install the plugin:

    ```bash
    path/to/elasticsearch_folder/bin/elasticsearch-plugin install analysis-icu
    ```

=== "Docker (security disabled)"

    ```bash
    docker run --name dots-es -d -p 9200:9200 \
      -e "discovery.type=single-node" \
      -e "xpack.security.enabled=false" \
      -e "xpack.security.http.ssl.enabled=false" \
      elasticsearch:8.12.1

    docker exec dots-es bash -c "bin/elasticsearch-plugin install analysis-icu"
    docker restart dots-es
    ```

Check that the server answers:

```bash
curl http://localhost:9200
```

## 2. The Python package

```bash
cd path/to/projects_folder/
git clone https://github.com/dots-suite/dots-cli-es.git
cd dots-cli-es
```

Make sure you are on Python 3.12, for example with `pyenv`:

```bash
pyenv shell 3.12
```

Create the virtual environment and install:

```bash
python3 -m venv your_venv_name
source your_venv_name/bin/activate
pip install .
```

This installs the `dots_es` package plus the **`dots-es-cli`** and **`dots-api`** console scripts.

For development (editable install and dev tooling):

```bash
pip install -e . -r requirements-dev.txt
```

!!! note "`requirements*.txt` are generated"
    Dependencies are declared in `pyproject.toml` and compiled with `pip-tools`. Do not edit the
    requirement files by hand:

    ```bash
    pip-compile pyproject.toml -o requirements.txt
    pip-compile --extra dev pyproject.toml -o requirements-dev.txt
    ```

## 3. ThunderDots from a local checkout (optional)

`thunderdots` is a **runtime dependency** of the indexing CLI, pulled from PyPI by default. To develop
against a local clone, install it in editable mode *after* the normal install:

```bash
pip install -e . -r requirements-dev.txt
pip install -e path/to/ThunderDots
```

Changes in the local repository then take effect immediately, without reinstalling.

!!! warning "Version specifier"
    The local version must satisfy the specifier declared in `pyproject.toml` (currently
    `thunderdots>=0.1.dev,<0.2`). A strict pin such as `==0.1.6` would conflict with a development
    snapshot like `0.1.dev37`.

Verify which copy is in use:

```bash
pip show thunderdots
```

## 4. uWSGI (servers only)

For servers running Python apps behind Nginx:

```bash
pip list --local          # is uWSGI already there?
pip install uwsgi         # may require: pip install wheel
```

The WSGI application is `flask_app:flask_app`.

## Next step

Head to the [Quick start](quickstart.md), or read the [Configuration](configuration.md) page first if
you need to point the CLI at a different DTS endpoint.
