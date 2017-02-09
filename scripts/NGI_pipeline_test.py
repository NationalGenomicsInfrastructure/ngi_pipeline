import os
import glob
import re
import sys
import argparse
import random
import requests
import json
import datetime
import shutil


def main(args):
    if hasattr(args, 'project_id'):
        # we are in delete mode
        print "deleting project {} from charon-dev".format(args.project_id)
        cleanCharon(args.project_id,args.url, args.token)
        print "  Done"
    elif hasattr(args, 'project_name'):
        # we are in create mode
        create_project(args.project_name,
                    args.fastq1,
                    args.fastq2,
                    args.facility,
                    args.FC,
                    args.symlinks,
                    args.token,
                    args.url
                    )
    else:
        print "Something strange is going on..."
        return 1
    return 0
    
def create_project(project_name, fastq1, fastq2, facility, FC, symlinks, token, url):
    # create fake project name and fake project idenitifier
    (project_name, project_identifier) = create_proj_name(project_name, facility, token, url)
    # create the data structure that will contains all the project, samples, libraries, runs
    charon_data = {}
    charon_data['projectid'] = project_identifier
    charon_data['name'] = project_name
    charon_data['sequencing_facility'] = "NGI-S" if facility == 'stockholm' else "NGI-U"
    charon_data['best_practice_analysis']="whole_genome_reseq"
    charon_data['status']='OPEN'
    charon_data['pipeline']='NGI'
    charon_data['samples']={}
    # now iterate over all FC I need to create
    sample_number = 1 # this needs to keep count of the samples
    FCs = []
    for i in xrange(FC):
        #create fake FC name
        FC_name = create_fake_FC()
        # create directory
        os.mkdir(FC_name)
        # create SampleSheet.csv
        touch(os.path.join(FC_name, "SampleSheet.csv"))
        # create folder Demultiplexing
        os.mkdir(os.path.join(FC_name, "Demultiplexing"))
        # create folder Demultiplexing/Reports
        os.mkdir(os.path.join(FC_name, "Demultiplexing", "Reports"))
        # create folder Demultiplexing/Stats
        os.mkdir(os.path.join(FC_name, "Demultiplexing", "Stats"))
        # create folder Demultiplexing/projet_name (replace "." with "_")
        project_name_underscore = project_name.replace(".", "_") # this should work with Uppsala also
        os.mkdir(os.path.join(FC_name, "Demultiplexing", project_name_underscore))
        # for each lane create a sample (make stockholm uppsala difference)
        print "producing FC: {}".format(FC_name)
        FCs.append(FC_name)
        for lane in (1,2,3,4,5,6,7,8):
            # create fake sample names, start easy, one sample per lane
            sample_name = create_sample_name(project_identifier, sample_number, facility)
            # create the sample folder
            sample_dir = os.path.join(FC_name, "Demultiplexing", project_name_underscore, "Sample_{}".format(sample_name))
            os.mkdir(sample_dir)
            sampinfo={ 'sampleid' : sample_name,
                    'received' : datetime.datetime.today().strftime("%Y-%m-%d"),
                    "total_autosomal_coverage" : "0",
                    "libs":{}
                    }
            library = "A" # only a single library
            sampinfo['libs'][library]={}
            sampinfo['libs'][library]['libprepid']=library
            sampinfo['libs'][library]['seqruns']={}
            
            sampinfo['libs'][library]['seqruns'][FC_name]={}
            sampinfo['libs'][library]['seqruns'][FC_name]['seqrunid']=FC_name
            sampinfo['libs'][library]['seqruns'][FC_name]['mean_autosomal_coverage']=0
            sampinfo['libs'][library]['seqruns'][FC_name]['sequencing_status']={1:"DONE"}
            sampinfo['libs'][library]['seqruns'][FC_name]['total_reads']=0
            # store sampinfo
            charon_data['samples'][sample_name]=sampinfo
            # now copy the fastq file
            fastq1_dest = os.path.join(sample_dir, "{}_S{}_L00{}_R1_001.fastq.gz".format(sample_name, lane, lane))
            fastq2_dest = os.path.join(sample_dir, "{}_S{}_L00{}_R2_001.fastq.gz".format(sample_name, lane, lane))
            if symlinks:
                os.symlink(fastq1, fastq1_dest)
                os.symlink(fastq2, fastq2_dest)
            else:
                shutil.copyfile(fastq1, fastq1_dest)
                shutil.copyfile(fastq2, fastq2_dest)
            
            sample_number += 1
    # now save everything to charon
    print "Updating charon-dev"
    writeProjectData(charon_data, token, url, facility)
    print "Now run the following:"
    print "PATH_TO_NGI=<path/to/ngi_pipeline>"
    print "PATH_TO_FC={}".format(os.getcwd())
    print "PATH_TO_DATA=<path/to/DATA>"
    for FC in FCs:
        print "python $PATH_TO_NGI/scripts/ngi_pipeline_start.py organize flowcell $PATH_TO_FC/{}".format(FC)
    print "python $PATH_TO_NGI/scripts/ngi_pipeline_start.py analyze project $PATH_TO_DATA/DATA/{}".format(project_identifier)
    print "python $PATH_TO_NGI/scripts/ngi_pipeline_start.py qc project $PATH_TO_DATA/DATA/{}".format(project_identifier)



def touch(file):
    open(file, "w").close()
    
    
def create_sample_name(project_identifier, sample_number, facility):
    sample_id = str(sample_number).zfill(3)
    if facility == 'stockholm':
        return "{}_{}".format(project_identifier, sample_id)
    else:
        return "U{}".format(sample_id)
    
        



def writeProjectData(data, token, base_url, facility):
    project=data
    samples=project.pop('samples', None)
    url=base_url+'/api/v1/project'
    projson=json.dumps(project)
    writeToCharon(projson, url, token)
    if facility == 'uppsala':
        # in uppsala case do not create samples, lib-preps, and runs level, they will be guessed while organising the FC
        return None
    for sid in samples.keys():
        libs=samples[sid].pop('libs', None)
        sampjson=json.dumps(samples[sid])
        url=base_url+'/api/v1/sample/'+project['projectid']
        writeToCharon(sampjson, url, token)
        for lib in libs.keys():
            seqruns=libs[lib].pop('seqruns', None)
            libjson=json.dumps(libs[lib])
            url=base_url+'/api/v1/libprep/'+project['projectid']+"/"+sid
            writeToCharon(libjson, url, token)
            for seqrun in seqruns.keys():
                seqjson=json.dumps(seqruns[seqrun])
                url=base_url+'/api/v1/seqrun/'+project['projectid']+"/"+sid+"/"+lib
                writeToCharon(seqjson, url, token)
    return None


def writeToCharon(jsonData, url, token):
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    r=session.post(url, headers=headers, data=jsonData)


def cleanCharon(pid, url, token):
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    r=session.delete(url+'/api/v1/project/'+pid, headers=headers)



def create_proj_name(project_name=None, facility='stockholm', token=None, url=None):
    project_id = ""
    while True:
        if project_name == None:
            if facility == 'stockholm':
                project_name_header = "J.Doe"
                year = generate_random_numbers(2)
                pnum = generate_random_numbers(2)
                project_name = "{}_{}_{}".format(project_name_header, year, pnum)
            else:
                project_name = "{}-{}".format(generate_random_chars(2), generate_random_numbers(4))
        else:
            project_name = project_name

        if facility == 'stockholm':
            project_id = "P{}".format(generate_random_numbers(3))
        else:
            project_id = project_name
        # check if this project already exists in charon
        if project_id_not_in_charon(project_id, token, url):
            break
    return (project_name, project_id)

def project_id_not_in_charon(project_id, token, url):
    # returns True if the project_id is not already present in charon. False otherwise
    session = requests.Session()
    headers = {'X-Charon-API-token': token, 'content-type': 'application/json'}
    # check if this project already exists
    return project_id not in [project['projectid'] for project in  session.get(url+'/api/v1/projects', headers=headers).json()['projects']]



def create_fake_FC():
    # create something like 160217_ST-E00201_0063_AHJHNYCCXX
    new = False
    while (not new):
        # date
        date = generate_random_numbers(6)
        # instrument name
        instrument = generate_random_instrument()
        # run id
        run_id = generate_random_numbers(4)
        # FC position
        FC_pos = random.choice(["A", "B"])
        # FC name
        FC_name = generate_random_chars(7) + 'XX'
        # now compose the name
        run_name = "{}_{}_{}_{}{}".format(date, instrument, run_id, FC_pos, FC_name)
        # check if this FC exists already
        if not os.path.exists(run_name):
            # this FC does not exists, fine
            new = True
    return run_name


def generate_random_chars(chars=0):
    return ''.join([random.choice('ABCEFGHILMNOPQRSTUVZ') for i in xrange(chars)])

def generate_random_numbers(numbers=0):
    return ''.join(str(random.randint(0,9)) for i in xrange(numbers))

def generate_random_instrument():
    instruments = ["ST-E00201", "ST-E00202", "ST-E00203", "ST-E00204", "ST-E00205"] # five instruments
    return  instruments[random.randint(0,4)]





if __name__ == '__main__':
    parser = argparse.ArgumentParser("""This script generates a test suite for NGI-pipeline and piper testing. It needs as input two fastq files (read 1 and read 2) and creates as many FC as specified. The enviorment variables need to point to charon-dev. It runs in two modalities, create and delete. The former creates the folder structure and the corresponding charon-dev entries, the latter deletes entries in charon-dev.""")
    # general options
    parser.add_argument('--facility', help="facility sequencing the project (stockholm, uppsala)", type=str, default="stockholm",
        choices=("stockholm", "uppsala"))
    parser.add_argument("-t", "--token", dest="token", default=os.environ.get('CHARON_API_TOKEN'),
            help="Charon API Token. Will be read from the env variable CHARON_API_TOKEN if not provided")
    parser.add_argument("-u", "--url", dest="url", default=os.environ.get('CHARON_BASE_URL'),
            help="Charon base url. Will be read from the env variable CHARON_BASE_URL if not provided")
    # add subparsers
    subparsers = parser.add_subparsers(help="subcommands")
    # delete options
    parser_delete = subparsers.add_parser('delete', help="Command for delete (i.e., clean) a projet in charon-dev")
    parser_delete.add_argument('--project-id', help="Project-id (e.g., P1021 or Uppsala project name) that will be deleted", type=str, required=True)
    # create options
    parser_create = subparsers.add_parser('create', help="Command for create  a projet in charon-dev")
    parser_create.add_argument('--fastq1', help="path to fastq file containing read 1", type=str,  required=True)
    parser_create.add_argument('--fastq2', help="path to fastq file containing read 2", type=str,  required=True)
    parser_create.add_argument('--FC', help="number of FC to be created", type=int,  required=True)
    parser_create.add_argument('--project-name', help="Name of the project that will be simulated (something like J.Doe_16_01)", type=str,
        default=None)
    parser_create.add_argument("--symlinks", help="instead of copying files creates symlinks", action='store_true', default=False)
    args = parser.parse_args()

    if not args.token :
        print( "No valid token found in arg or in environment. Exiting.")
        sys.exit(-1)
    if not args.url:
        print( "No valid url found in arg or in environment. Exiting.")
        sys.exit(-1)

    if 'dev' not in args.url:
        print( "Something tells me that you are not using charon-dev.... {}".format(args.url))
        sys.exit(-1)


    main(args)




