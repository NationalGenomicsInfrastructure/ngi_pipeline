import os
import sys
from shutil import copy

from ngi_pipeline.utils.parsers import get_sample_numbers_from_samplesheet
from ngi_pipeline.tests.generate_test_data import generate_run_id


def paths_from_samplesheet(samplesheet_file):

    samples = get_sample_numbers_from_samplesheet(samplesheet_file)
    for sample in samples:
        yield [
            os.path.join(
                sample[1],
                f"Sample_{sample[3]}",
                f"{sample[2]}_{sample[0]}_L{sample[5]:03d}_R{r}_001.fastq.gz")
            for r in (1, 2)]


def fc_from_samplesheet(samplesheet_file, base_dir, fc_name=None):
    fc_name = fc_name or generate_run_id()
    fc_dir = os.path.join(base_dir, fc_name)

    # create the fc and copy the samplesheet there
    os.mkdir(fc_dir)
    copy(samplesheet_file, fc_dir)

    # create the Unaligned directory to hold the fastq files
    unaligned = os.path.join(fc_dir, "Unaligned")
    os.mkdir(unaligned)

    # iterate over the fastq files to be created and touch them
    for fq_files in paths_from_samplesheet(samplesheet_file):
        for fqfile in fq_files:
            fqdir = os.path.join(unaligned, os.path.dirname(fqfile))
            os.makedirs(fqdir, exist_ok=True)
            open(os.path.join(fqdir, os.path.basename(fqfile)), "w").close()


if __name__ == '__main__':
    samplesheet_file = sys.argv[1]
    base_dir = sys.argv[2]
    fc_name = None if len(sys.argv) < 4 else sys.argv[3]
    fc_from_samplesheet(samplesheet_file, base_dir, fc_name=fc_name)
