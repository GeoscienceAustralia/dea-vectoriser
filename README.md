
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/GeoscienceAustralia/dea-vectoriser?label=latest%20version)
[![](https://img.shields.io/codecov/c/github/GeoscienceAustralia/dea-vectoriser)](https://codecov.io/gh/GeoscienceAustralia/dea-vectoriser)
[![](https://img.shields.io/docker/image-size/geoscienceaustralia/dea-vectoriser)](https://hub.docker.com/r/geoscienceaustralia/dea-vectoriser)
[![](https://img.shields.io/docker/v/geoscienceaustralia/dea-vectoriser)](https://hub.docker.com/r/geoscienceaustralia/dea-vectoriser)

# Digital Earth Australia Vectoriser

A brief description of what this project does and who it's for


## Features

- Generates Water Observations Vectors
- Read and write data from S3
- Configurable output format (GeoPackage, GeoJSON, Shapefile)
- Reads STAC notifications from an SQS queue to discover rasters to process

## Quick Start

### Install dependencies
This application depends on the following packages:
1. Conda - https://docs.conda.io/en/latest/
1. Mamba - https://mamba.readthedocs.io/en/latest/

Linux:
- TODO

Windows:
- TODO

OSX:
```
  brew install anaconda
  conda install -n base -c conda-forge mamba
  conda init zsh #depends on user shell (bash, zsh, etc)
```

### Setup Dev environment
Then, all platforms:

``` bash
  conda activate
  mamba env create --file environment.yaml --name dea-vectoriser
  conda activate dea-vectoriser
  pip install -e .
```

## Deployment



To deploy this project run

```bash
  make build-package
  make build-image
```

## How it works

DEA Vectoriser

  

  
## Running Tests

To run tests, run the following command

```bash
  pytest
```

  
## License

[Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)

  
