
.PHONY: build-package build-image

VERSION := $(shell python -m setuptools_scm | awk '{print $$3}')

build-package:
	rm -rf dist/*
	python -m build

build-image: #build-package
	docker buildx build -t geoscienceaustralia/dea-vectoriser .
	docker tag geoscienceaustralia/dea-vectoriser geoscienceaustralia/dea-vectoriser:$(VERSION)

run-one-wofs:
	dea-vectoriser run-from-s3-url --destination s3://dea-public-data-dev/carsa/vector_ba s3://dea-public-data-dev/derivative/ga_s2_wo_3/0-0-1/53/HQT/2021/07/20/20210720T015707/ga_s2_wo_3_53HQT_2021-07-20_nrt.stac-item.json --algorithm wofs

run-one-burns:
	dea-vectoriser run-from-s3-url --destination s3://dea-public-data-dev/carsa/vector_ba s3://dea-public-data-dev/derivative/ga_s2_ba_provisional_3/1-6-0/56/JLT/2021/09/06/ga_s2_ba_provisional_3_56JLT_2021-09-06_interim.stac-item.json --algorithm burns

run-burns-control:
	dea-vectoriser run-from-s3-url --destination s3://dea-public-data-dev/carsa/vector_ba s3://dea-public-data-dev/derivative/ga_s2_ba_bm_3/1-6-0/56/JKT/2021/07/31/20210731T011212/ga_s2_ba_bm_3_56JKT_2021-07-31_interim.stac-item.json --algorithm burns
