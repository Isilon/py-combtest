# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
DOCSOURCEDIR  = .
PYSOURCEDIR   = ../combtest
BUILDDIR      = ../docs

# User-friendly check for sphinx-build
ifeq ($(shell which $(SPHINXBUILD) >/dev/null 2>&1; echo $$?), 1)
$(error The '$(SPHINXBUILD)' command was not found. Make sure you have Sphinx installed, then set the SPHINXBUILD environment variable to point to the full path of the '$(SPHINXBUILD)' executable. Alternatively you can add the directory with the executable to your PATH. If you don't have Sphinx installed, grab it from http://sphinx-doc.org/)
endif

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(DOCSOURCEDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS)
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .


.PHONY: help clean html autodoc

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  clean"
	@echo "  html       to make standalone HTML files"
	@echo "  autodoc    to make API docs from source"

clean:
	rm -rf $(BUILDDIR)/*

autodoc:
	@echo "TODO"

html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(DOCSOURCEDIR) $(BUILDDIR)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)."

