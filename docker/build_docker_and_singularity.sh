#! /bin/bash -l

## Script for building a docker image and subsequently a singularity image
## you'll probably need Singularity 3+ in order to use docker-daemon

# remove any pre-existing singularity images
rm -f ngi_pipeline.sif

# build docker image
docker build -t ngi_pipeline:latest -f docker/Dockerfile .

# build singularity image from docker imnage
singularity build ngi_pipeline.sif docker-daemon:ngi_pipeline:latest
