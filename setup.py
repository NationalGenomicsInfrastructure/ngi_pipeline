#!/usr/bin/env python
import glob

from setuptools import setup, find_packages

try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

setup(name="ngi_pipeline",
      author = "Science for Life Laboratory",
      author_email = "mario@scilifelab.se",
      description = "Infrastructure/analysis pipeline scripts.",
      license = "MIT",
      url="https://github.com/scilifelab/scilifelab_pipeline",
      version="0.1.0",
      install_requires=install_requires,
      scripts = glob.glob('scripts/*py'),
      packages=find_packages()
      )
