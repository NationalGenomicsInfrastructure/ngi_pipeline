#! /bin/bash

cat requirements.txt requirements-dev.txt |while read p
do
  conda install -c defaults -c conda-forge -c bioconda -y "$p"
done

