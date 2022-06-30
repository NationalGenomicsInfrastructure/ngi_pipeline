#! /bin/bash -l

## example script for interacting with the singularity image and setting the
## appropriate environment variables and bind points
##
## an example config is present under /conf
##
## note that for e.g. development, you can bind the ngi_pipeline source code
## under /ngi_pipeline and it will run off that
##
## example usage: singularity_ngi_pipeline.sh ngi_pipeline_start.py -h
##

NGI_CONFIG=/conf/ngi_config.yaml
CHARON_BASE_URL=this-is-charon-url
CHARON_API_TOKEN=this-is-charon-token

singularity exec --bind \
/lupus/ngi/staging/latest/sw:/sw,\
/lupus/ngi/staging/latest/resources:/resources,\
/lupus/ngi/staging/wildwest/ngi2016001:/data/ngi2016000 \
ngi_pipeline.sif $@

# --bind /path/to/dev/src/code:/ngi_pipeline
