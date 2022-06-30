#!/bin/env python

import argparse
import importlib


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--engine", required=True)

    # E.g. piper
    engine = parser.parse_args().engine.lower()

    module = "ngi_pipeline.engines.{}".format(engine)
    try:
        imported_module = importlib.import_module(module)
    except ImportError:
        # try suffixing the engine with "_ngi"
        module = "{}_ngi".format(module)
        imported_module = importlib.import_module(module)

    update_function = imported_module.local_process_tracking.update_charon_with_local_jobs_status
    update_function()
