
.PHONY: build-package build-image

VERSION := $(shell python -m setuptools_scm | awk '{print $$3}')

build-package:
	rm -rf dist/*
	pip install build
	python -m build

build-image: #build-package
	docker buildx build -t geoscienceaustralia/dea-vectoriser .
	docker tag geoscienceaustralia/dea-vectoriser geoscienceaustralia/dea-vectoriser:$(VERSION)
