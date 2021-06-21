
.PHONY: build-package build-image

VERSION := $(shell python -m setuptools_scm | awk '{print $3}')

build-package:
	python -m build

build-image:
	docker buildx build -t geoscienceaustralia/dea-vectoriser .
	docker tag geoscienceaustralia/dea-vectoriser geoscienceaustralia/dea-vectoriser:$(VERSION)
