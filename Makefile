
.PHONY: build-package build-image

build-package:
	python -m build

build-image:
	docker buildx build -t datacube-vectoriser .
