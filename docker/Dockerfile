FROM continuumio/miniconda:latest

COPY docker/ngi_config.yaml /conf/ngi_config.yaml
COPY . /ngi_pipeline
WORKDIR /ngi_pipeline

RUN \
  conda upgrade -y pip && \
  /bin/sh docker/conda_install.sh && \
  pip install -r requirements-dev.txt && \
  python setup.py develop

ENV NGI_CONFIG /conf/ngi_config.yaml
ENV CHARON_BASE_URL pass-this-into-the-container-at-runtime
ENV CHARON_API_TOKEN pass-this-into-the-container-at-runtime

RUN \
  mkdir -p /data/ngi2016000/private/db && \
  mkdir -p /data/ngi2016000/private/log && \
  mkdir -p /data/ngi2016000/incoming && \
  mkdir -p /data/ngi2016000/nobackup/NGI && \
  mkdir -p /sw && \
  mkdir -p /resources
