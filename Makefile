MKDOCS       ?= mkdocs
DOCS_CONFIG  := mkdocs/mkdocs.yml
DOCS_SITE    := mkdocs/site

.PHONY: help docs.install docs.lock docs.serve docs.build docs.build-dev docs.check docs.clean docs.deploy

help:
	@echo "Documentation targets:"
	@echo "  docs.install    install the MkDocs toolchain (mkdocs/requirements.txt)"
	@echo "  docs.lock       recompile mkdocs/requirements.txt from mkdocs/requirements.in"
	@echo "  docs.serve      live-reload server on http://127.0.0.1:8000"
	@echo "  docs.build      build the site in strict mode (fails on broken links)"
	@echo "  docs.build-dev  build without --strict"
	@echo "  docs.check      alias of docs.build, for CI"
	@echo "  docs.clean      remove the generated site"
	@echo "  docs.deploy     publish to the gh-pages branch (manual fallback)"

docs.install:
	pip install -r mkdocs/requirements.txt

docs.lock:
	pip-compile mkdocs/requirements.in -o mkdocs/requirements.txt

docs.serve:
	$(MKDOCS) serve -f $(DOCS_CONFIG)

docs.build:
	$(MKDOCS) build -f $(DOCS_CONFIG) --strict

docs.build-dev:
	$(MKDOCS) build -f $(DOCS_CONFIG)

docs.check: docs.build

docs.clean:
	rm -rf $(DOCS_SITE)

docs.deploy:
	$(MKDOCS) gh-deploy -f $(DOCS_CONFIG) --force
