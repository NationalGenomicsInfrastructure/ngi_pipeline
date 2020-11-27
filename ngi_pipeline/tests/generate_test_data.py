import datetime
import os
import random
import re
import string
import tempfile


## TODO clean up naming conventions (fcid, barcode, run_id, identifier, barcode)

def generate_project_name():
    """Generate a project name in the form Y.Mom_12_34
    """
    return "{0}.{1}_{2}_{3}".format(
            random.choice(string.ascii_uppercase),
            "".join([random.choice(string.ascii_lowercase) for i in range(6) ]).capitalize(),
            random.randint(12,14),
            random.randint(11,99))

def generate_sample_name(project_id=None):
    """Generate a sample name in the form P123_456
    """
    if not project_id: project_id = random.randint(100,999)
    return "P{0}_{1}".format(project_id, random.randint(101,199))


def generate_flowcell_id():
    """Generate a flowcell id in the format AABC123CXX
    """
    return "{0}{1}CXX".format(random.choice("AB"),
            "".join([random.choice(string.ascii_uppercase + string.digits) for i in range(6)]))


def generate_instrument_id(prefix="SN"):
    """Generate an instrument ID in the format SN#### or so
    """
    return "{0}{1}".format(prefix, str(random.randint(101,9999)))


def generate_barcode(length=6):
    """Generate a nucleotide barcode
    """
    return generate_nucleotide_sequence(seq_length=length)


def generate_nucleotide_sequence(seq_length, alphabet="AGCT"):
    """Generate a random string of specified length ahd nucleotides sampled
    from the specified alphabet
    """
    if not alphabet:
        alphabet = "AGCT"
    return "".join([ random.choice(alphabet) for i in range(seq_length) ])


def generate_sample_file_name(sample_name=None, barcode=None, lane=None, read_num=1):
    """Generate a CASAVA 1.8+-style sample file name in the format
    P567_102_AAAAAA_L001_R1_001.fastq.gz
    """
    if not sample_name: sample_name = generate_sample_name()
    if not barcode: barcode = generate_barcode()
    if not lane: lane = random.randint(1,8)
    return "{sample_name}_{barcode}_L00{lane}_R{read_num}_001.fastq.gz".format(**locals())


def generate_paired_sample_file_names(sample_name=None, barcode=None, lane=None):
    if not sample_name: sample_name = generate_sample_name()
    if not barcode: barcode = generate_barcode()
    if not lane: lane = random.randint(1,8)
    return [ generate_sample_file_name(sample_name, barcode, lane, read_num) for
             read_num in (1,2) ]


def generate_run_id(date=None, instrument_id=None, fcid=None):
    """Generate a run identifier in the format:
        <YYMMDD>_<instrument_id>_0<nnn>_<FCID>
    """
    if not date: date = datetime.date.today().strftime("%y%m%d")
    if not instrument_id: instrument_id = generate_instrument_id()
    if not fcid: fcid = generate_flowcell_id()
    return "{date}_{instrument_id}_0{number}_{fcid}".format(date=date,
                                                              instrument_id=instrument_id,
                                                              number=random.randint(101,999),
                                                              fcid=fcid)


def generate_identifier(date=None, flowcell_id=None):
    """Generate a date_barcode identifier in the format 140704_ABC123CXX
    """
    if not date: date = datetime.date.today().strftime("%y%m%d")
    if not flowcell_id: flowcell_id = generate_flowcell_id()
    return "{}_{}".format(date, flowcell_id)


def create_demultiplexed_flowcell():
    """
    140704_D00123_0321_BC423WACXX/
    |--- RunInfo.xml
    |--- runParameters.xml
    |--- SampleSheet.csv
    |--- Unaligned
         |--- Project_J__Doe_14_01
              |--- Sample_P123_456
                   |--- P123_456_AGCTGC_1_R1_001.fastq.gz
                   |--- P123_456_AGCTGC_1_R2_001.fastq.gz
    """
    run_id = generate_run_id()
    project_name = generate_project_name()
    sample_name = generate_sample_name()
    run_info_xml_text = generate_RunInfo()
    run_parameters_xml_text = generate_runParameters()
    tmp_dir = tempfile.mkdtemp()
    run_dir = os.path.join(tmp_dir, run_id)
    run_samplesheet = os.path.join(run_dir, "SampleSheet.csv")
    run_info_xml_file = os.path.join(run_dir, "RunInfo.xml")
    run_parameters_xml_file = os.path.join(run_dir, "runParameters.xml")
    unaligned_dir = os.path.join(run_dir, "Unaligned")
    project_dir = os.path.join(unaligned_dir, project_name)
    sample_dir = os.path.join(project_dir, "Sample_{}".format(sample_name))
    sample_samplesheet = os.path.join(sample_dir, "SampleSheet.csv")
    # Created the whole tree, run_dir/unaligned_dir/project_dir/sample_dir
    os.makedirs(sample_dir)
    generate_sample_file_name(sample_name)
    # Touch files
    open(run_samplesheet, 'w').close()
    open(sample_samplesheet, 'w').close()
    for fq in generate_paired_sample_file_names(sample_name = sample_name):
        fq_file_path = os.path.join(sample_dir, fq)
        open(fq_file_path, 'w').close()
    # Write files
    with open(run_info_xml_file, 'w') as f:
        f.writelines(run_info_xml_text)
    with open (run_parameters_xml_file, 'w') as f:
        f.writelines(run_parameters_xml_text)
    # Generate Basecall_Stats?
    return run_dir


def generate_runParameters():
    """Generate a dummy runParameters.xml file.
    This contains only the "FCPosition" parameter."
    """
    return (
    '<?xml version="1.0"?>\n'
    '<RunParameters xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
    '  <Setup>\n'
    '    <FCPosition>{}</FCPosition>\n'
    '  </Setup>\n'
    '</RunParameters>\n').format(random.choice("AB"))


def generate_RunInfo(run_id=None, fcid=generate_flowcell_id(),
                     date=datetime.date.today().strftime("%y%m%d"),
                     instrument_id=generate_instrument_id()):
    """Generate a dummy RunInfo.xml file. This contains only the "Flowcell",
    "Date", and "Instrument" parameters.
    """
    run_id = generate_run_id(date=date, instrument_id=instrument_id, fcid=fcid)
    if run_id:  # User submitted a run id which we must parse
        try:
            date, instrument_id, _, fcid = run_id.split("_")
        except ValueError as e:
            raise ValueError("Improperly formatted Run ID: should be in the form "
                             "<date>_<instrument_id>_<nnnn>_<A/B+FCID>")
    else:   # We make our own rules
        date=datetime.date.today()
        instrument_id=generate_instrument_id()
        fcid=generate_flowcell_id()
        run_id = generate_run_id(date=date, instrument_id=instrument_id, fcid=fcid)
    # I suppose it would be better here to make some kind of dict->xml function
    # So you feel free to go ahead and write that for me
    return (
    '<?xml version="1.0">\n'
    '<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="2">\n'
    '  <Run Id="{run_id}" Number="01">\n'
    '    <Flowcell>{fcid}</Flowcell>\n'
    '    <Instrument>{instrument_id}</Instrument>\n'
    '    <Date>{date}</Date>\n'
    '  </Run>\n'
    '</RunInfo>\n').format(run_id=run_id, fcid=fcid, instrument_id=instrument_id,
                         date=date)


def create_project_structure(project_name=generate_project_name(),
                               run_id=generate_run_id(),
                               sample_name=generate_sample_name(),
                               empty_files=False):
    """Create a project directory structure complete with fastq files;
    creates empty files if empty_files is True.
    """
    raise NotImplementedError
