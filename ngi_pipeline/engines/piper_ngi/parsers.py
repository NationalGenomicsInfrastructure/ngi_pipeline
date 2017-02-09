"""Here we will keep results parsers for the various output files produced by Piper."""
import glob
import os
import sys

from collections import namedtuple
from ngi_pipeline.log.loggers import minimal_logger


LOG = minimal_logger(__name__)

## Maybe define parameters
def parse_results_for_workflow(workflow_name, *args, **kwargs):
    parser_fn_name = "parse_{}".format(workflow_name)
    try:
        parser_function = getattr(sys.modules[__name__], parser_fn_name)
    except AttributeError as e:
        error_msg = 'Workflow "{}" has no associated parser implemented.'.format(workflow_name)
        LOG.error(error_msg)
        raise NotImplementedError(error_msg)
    return parser_function(*args, **kwargs)

def parse_qualimap_reads(genome_results_file):
    globals_section= False
    with open(genome_results_file, 'r') as f:
        for line in f:
            if line.startswith('>>>>>>> Globals'):
                globals_section= True
                continue
            if globals_section:
                if 'number of reads' in line:
                    return int(line.split('=')[-1].strip().replace(',',''))
    return 0

    

def parse_qualimap_coverage(genome_results_file):
    autosomal_cov_length = 0
    autosomal_cov_bases = 0
    coverage_section = False
    with open(genome_results_file, 'r') as f:
        for line in f:
            if line.startswith('>>>>>>> Coverage per contig'):
                coverage_section = True
                continue
            if coverage_section:
                line = line.strip()
                if line:
                    sections = line.split()
                    if sections[0].isdigit() and int(sections[0]) <= 22:
                        autosomal_cov_length += float(sections[1])
                        autosomal_cov_bases += float(sections[2])
        if autosomal_cov_length and autosomal_cov_bases:
            return autosomal_cov_bases / autosomal_cov_length
        else:
            return 0.0


def parse_mean_autosomal_coverage_for_sample(piper_qc_dir, sample_id):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample as gleaned from the qualimapReport.html present in
    piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :returns: The mean autosomal coverage
    :rtype: int
    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    return parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id)


def parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id=None, fcid=None):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample OR seqrun (if seqrun_id is passed) as gleaned from
    the qualimapReport.html present in piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)
    :param str seqrun_id: The run id (e.g. 140821_D00458_0029_AC45JGANXX) (optional) (specify either this or fcid)
    :param str fcid: The FCID (optional) (specify either this or seqrun_id)

    :returns: The mean autosomal coverage
    :rtype: int

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    try:
        if seqrun_id and fcid and (fcid != seqrun_id.split("_")[3]):
            raise ValueError(('seqrun_id and fcid both passed as arguments but do not '
                              'match (seqrun_id: "{}", fcid: "{}")'.format(seqrun_id, fcid)))
        if seqrun_id:
            piper_run_id = seqrun_id.split("_")[3]
        elif fcid:
            piper_run_id = fcid
        else:
            piper_run_id = None
    except IndexError:
        raise ValueError('Can\'t parse FCID from run id ("{}")'.format(seqrun_id))
    # Find all the appropriate files
    try:
        os.path.isdir(piper_qc_dir) and os.listdir(piper_qc_dir)
    except OSError as e:
        raise OSError('Piper result directory "{}" inaccessible when updating stats to Charon: {}.'.format(piper_qc_dir, e))
    piper_qc_dir_base = "{}.{}.{}".format(sample_id, (piper_run_id or "*"), sample_id)
    piper_qc_path = "{}*/".format(os.path.join(piper_qc_dir, piper_qc_dir_base))
    piper_qc_dirs = glob.glob(piper_qc_path)
    if not piper_qc_dirs: # Something went wrong, is the sample name with a hyphen or with an underscore ?
        piper_qc_dir_base = "{}.{}.{}".format(sample_id.replace('_', '-', 1), (piper_run_id or "*"), sample_id.replace('_', '-', 1))
        piper_qc_path = "{}*/".format(os.path.join(piper_qc_dir, piper_qc_dir_base))
        piper_qc_dirs = glob.glob(piper_qc_path)
        if not piper_qc_dirs: # Something went wrong in the alignment or we can't parse the file format
            raise OSError('Piper qc directories under "{}" are missing or in an unexpected format when updating stats to Charon.'.format(piper_qc_path))
    mean_autosomal_coverage = 0
    # Examine each lane and update the dict with its alignment metrics
    for qc_lane in piper_qc_dirs:
        genome_result = os.path.join(qc_lane, "genome_results.txt")
        # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
        if not os.path.isfile(genome_result):
            raise OSError('File "genome_results.txt" is missing from Piper result directory "{}"'.format(piper_qc_dir))
        # Get the alignment results for this lane
        mean_autosomal_coverage += parse_qualimap_coverage(genome_result)
    return mean_autosomal_coverage



def parse_genotype_concordance(genotype_concordance_file):
    genotype_concordance_file = os.path.realpath(genotype_concordance_file)
    concordance_data = []
    gt_values_list = []
    with open(genotype_concordance_file, 'r') as f:
        for line in iter(f.readline, ''):
            header_location = None
            if line.startswith("#:GATKTable:GenotypeConcordance_Summary"):
                header_values = [h.strip() for h in f.readline().strip().split('  ') if h.strip()]
                header_values = [h.lower().replace("-", "_").replace(" ", "_") for h in header_values]
                GTValue = namedtuple('GTValue', header_values)
                f.readline() # Skip first ("ALL") summary line
                data_location = f.tell()
                break
        else:
            raise ValueError('Unable to find genotype concordance summary '
                             'section in genotype file "{}"'.format(genotype_concordance_file))
    with open(genotype_concordance_file, 'r') as f:
        f.seek(data_location)
        for line in iter(f.readline, ''):
            if line.strip() != "":
                try:
                    gt_values_list.append(GTValue._make(line.strip().split()))
                except TypeError as e:
                    LOG.error('Unable to parse genotype concordance line "{}"; number '
                              'of data fields does not match number of header fields '
                              'fields ({}); skipping'.format(" ".join(line.strip().split()), e))
                    continue
            else:
                break
    samples_gtc_dict = {}
    for gt_entry in gt_values_list:
        try:
            samples_gtc_dict[gt_entry.sample] = float(gt_entry.overall_genotype_concordance)
        except ValueError as e:
            LOG.error('Unable to parse overall genotype concordance '
                      'value for sample "{}" (value "{}" is not a '
                      'number)'.format(gt_entry.sample,
                                       gt_entry.overall_genotype_concordance))
            continue
    return samples_gtc_dict

def parse_deduplication_percentage(deduplication_file):

    duplication_percentage=0
    with open(deduplication_file, 'r') as f:
        for line in iter(f.readline, ''):
            if "## METRICS CLASS" in line and "picard.sam.DuplicationMetrics" in line:
                try:
                    headers=f.readline()
                    values=f.readline()
                    duplication_rate=values.split()[headers.split().index("PERCENT_DUPLICATION")]
                    duplication_percentage=float(duplication_rate)*100
                except:
                    LOG.error("Unable to parse deduplication rate")
                    continue

    return duplication_percentage
