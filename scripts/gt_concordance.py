#!/usr/bin/env python
from __future__ import print_function
import sys
import os
import re
import subprocess
import shutil

import click
import yaml
import vcf
import pyexcel_xlsx

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config

log = minimal_logger(os.path.basename(__file__))

@click.group()
@with_ngi_config
@click.pass_context
@click.option('--config', '-c', 'custom_config', help='Path to a config file', type=click.Path())
def cli(context, config_file_path, config, custom_config=None):
    # check first if config file is specified
    if custom_config is not None:
        log.info('Using custom config file: {}'.format(os.path.abspath(custom_config)))
        if not os.path.exists(custom_config):
            log.error('Config file does not exist!')
            exit(1)
        with open(custom_config, 'r') as config_file:
            config = yaml.load(config_file) or {}
            config = config.get('gt_concordance') or {}
    # if not, using ngi_pipeline config
    else:
        log.info('Using ngi_pipeline config file: {}'.format(os.path.abspath(config_file_path)))
        gt_concordance_config = config.get('gt_concordance')
        if gt_concordance_config is None:
            log.error('NGI_CONFIG requires gt_concordance section!')
            exit(1)
        analysis_config = config.get('analysis')
        if analysis_config is None:
            log.error('NGI_CONFIG missing analysis section!')
            exit(1)
        ANALYSIS_PATH = os.path.join(analysis_config.get('base_root'), analysis_config.get('sthlm_root'), analysis_config.get('top_dir'), 'ANALYSIS')
        if 'None' in ANALYSIS_PATH:
            log.error('base_root, sthlm_root and top_dir must be specified! Check the config file')
            exit(1)
        gt_concordance_config['ANALYSIS_PATH'] = ANALYSIS_PATH
        config = gt_concordance_config
    context.obj = config

@cli.command()
@click.pass_context
def parse_xl_files(context):
    config = context.obj
    if is_xl_config_ok(config):
        files_to_archive = []
        samples_to_update = []

        # parsing snps file
        snps_data = parse_maf_snps_file(config)

        # checking xl files
        XL_FILES_PATH = config.get('XL_FILES_PATH')
        log.info('Looking for .xlsx files in {}'.format(XL_FILES_PATH))
        xl_files = [os.path.join(XL_FILES_PATH, filename) for filename in os.listdir(XL_FILES_PATH)]
        log.info('{} files found'.format(len(xl_files)))

        # parsing xl files
        for xl_file in xl_files:
            log.info("Parsing file: {}".format(os.path.basename(xl_file)))
            xl_file_data = parse_xl_file(config, xl_file)
            # returns sample name for each created gt file
            gt_samples = create_gt_files(config, xl_file_data, snps_data, os.path.basename(xl_file))
            samples_to_update += gt_samples
            log.info('{} files have been created in {}/<project>/piper_ngi/03_genotype_concordance'.format(len(gt_samples), config.get('ANALYSIS_PATH')))

            # if ALL gt files were created, archive xl_file
            if len(gt_samples) == len(xl_file_data):
                files_to_archive.append(xl_file)
            # otherwise we keep xl_file in incoming
            else:
                log.warning('File will not be archived: {}'.format(xl_file))

        # archive files
        archived = []
        XL_FILES_ARCHIVED = config.get('XL_FILES_ARCHIVED')
        for xl_file in files_to_archive:
            try:
                shutil.move(xl_file, XL_FILES_ARCHIVED)
            except Exception as e:
                log.error('Cannot move file {} to {}'.format(xl_file, XL_FILES_ARCHIVED))
                log.error('Error says: {}'. format(str(e)))
            else:
                archived.append(xl_file)
        log.info('{} files have been archived'.format(len(archived)))

        log.info('Updaing Charon')

        # update charon
        updated_samples = []
        for sample in samples_to_update:
            error = update_gt_status_in_charon(sample, 'AVAILABLE')
            if error is None:
                updated_samples.append(sample)
            else:
                log.error('Sample has not been updated in Charon: {}'.format(sample))
                log.error('Error says: {}'.format(error))
        log.info('{}/{} samples have been updated in Charon'.format(len(updated_samples), len(samples_to_update)))


def create_gt_files(config, xl_file_data, snps_data, xl_file_name):
    processed_samples = []
    for sample_id in xl_file_data:
        project_id = sample_id.split('_')[0]
        # create .gt file for each sample
        output_path = os.path.join(config.get('ANALYSIS_PATH'), project_id, 'piper_ngi/03_genotype_concordance')
        if not os.path.exists(output_path):
            log.warning('Output path does not exist! {}'.format(output_path))
            log.warning('File {}.gt will not be created!'.format(sample_id))
            continue

        filename = os.path.join(output_path, '{}.gt'.format(sample_id))
        if os.path.exists(filename):
            source_xl_file = open(filename, 'r').readlines()[0].replace('#', '').strip()
            if source_xl_file == xl_file_name:
                log.warning('File {} already exists. Skipping'.format(os.path.basename(filename)))
                processed_samples.append(sample_id)
                continue
            else:
                log.error('COLLISION! Sample {} exists in 2 excel files: {} and {}'.format(sample_id, xl_file_name, source_xl_file))
                log.error('To continue, move existing .gt file and restart again')
                exit(1)
        # create file and write data
        with open(filename, 'w+') as output_file:
            output_file.write('# {}\n'.format(xl_file_name))
            for rs, alleles in xl_file_data[sample_id].items():
                # checking if snps_data contains such position:
                if rs not in snps_data:
                    log.warning('rs_position {} not found in snps_file!!'.format(rs))
                    log.warning('Skipping')
                    continue
                chromosome, position, rs_position, reference, alternative = snps_data.get(rs)
                output_file.write("{} {} {} {} {} {} {}\n".format(chromosome, position, rs, reference, alternative, alleles[0], alleles[1]))
            processed_samples.append(sample_id)
    return processed_samples

def parse_xl_file(config, xl_file):
    genotype_data = {}
    data = pyexcel_xlsx.get_data(xl_file)
    data = data.get('HaploView_ped_0') # sheet name
    # getting list of lists
    header = data[0]
    data = data[1:]
    for row in data:
        # row[1] is always sample name. If doesn't match NGI format - skip.
        if len(row) == 0:
            continue
        if not re.match(r"^ID\d+-P\d+_\d+", row[1]):
            continue
        # sample has format ID22-P2655_176
        sample_id = row[1].split('-')[-1]
        # if the same sample occurs twice in the same file, will be overwriten
        if sample_id not in genotype_data:
            genotype_data[sample_id] = {}
        else:
            log.warning('Sample {} has been already parsed from another (or this) file. Overwriting'.format(sample_id))
        # rs positions start from 9 element. hopefully the format won't change
        for rs_id in header[9:]:
            rs_index = header.index(rs_id)
            allele1, allele2 = row[rs_index].split()
            genotype_data[sample_id][rs_id] = [allele1, allele2]
    return genotype_data

def is_xl_config_ok(config):
    # checking config
    XL_FILES_PATH = config.get('XL_FILES_PATH')
    if XL_FILES_PATH is None:
        log.error("config file missing XL_FILES_PATH argument")
        exit(1)
    if not os.path.exists(XL_FILES_PATH):
        log.error("Path to excel files does not exist! Path: {}".format(XL_FILES_PATH))
        exit(1)
    #
    ANALYSIS_PATH = config.get('ANALYSIS_PATH')
    if ANALYSIS_PATH is None:
        log.error("config file missing ANALYSIS_PATH")
        exit(1)
    if not os.path.exists(ANALYSIS_PATH):
        log.error('Analysis path does not exist! Path: {}'.format(ANALYSIS_PATH))
        exit(1)

    XL_FILES_ARCHIVED = config.get('XL_FILES_ARCHIVED')
    if XL_FILES_ARCHIVED is None:
        log.error('config file missing XL_FILES_ARCHIVED')
    if not os.path.exists(XL_FILES_ARCHIVED):
        log.error('Path does not exist! Path: {}'.format(XL_FILES_ARCHIVED))
        exit(1)

    SNPS_FILE = config.get('SNPS_FILE')
    if SNPS_FILE is None:
        log.error('config file missing SNPS_FILE')
        exit(1)
    if not os.path.exists(SNPS_FILE):
        log.error('SNPS file does not exist! Path: {}'.format(SNPS_FILE))
        exit(1)

    xl_files = [os.path.join(XL_FILES_PATH, filename) for filename in os.listdir(XL_FILES_PATH) if '.xlsx' in filename]
    if not xl_files:
        log.error('No .xlsx files found! Terminating')
        exit(1)

    return True

def parse_maf_snps_file(config):
    SNPS_FILE = config.get('SNPS_FILE')
    snps_data = {}
    if os.path.exists(SNPS_FILE):
        with open(SNPS_FILE) as snps_file:
            lines = snps_file.readlines()
            for line in lines:
                chromosome, position, rs_position, reference, alternative = line.split()
                snps_data[rs_position] = [chromosome, position, rs_position, reference, alternative]
    return snps_data

@cli.command()
@click.argument('sample')
@click.option('--force', '-f', is_flag=True, default=False, help='If not specified, will keep existing vcf files and use them to check concordance. Otherwise overwrite')
@click.pass_context
def genotype_sample(context, sample, force):
    if is_config_file_ok():
        concordance = run_genotype_sample(sample, force)
        if concordance is None:
            log.error('Failed to genotype sample: {}'.format(sample))
            error = update_gt_status_in_charon(sample, status='FAILED')
            if error:
                log.error('Sample has not been updated in Charon: {}'.format(sample))
                log.error('Error says: {}'.format(error))
        else:
            error = update_gt_status_in_charon(sample, status='DONE', concordance=concordance)
            if error:
                log.error('Sample has not been updated in Charon: {}'.format(sample))
                log.error('Error says: {}'.format(error))
            print('Sample name\t % concordance')
            print('{}:\t {}'.format(sample, concordance))

@click.pass_context
def run_genotype_sample(context, sample, force=None):
    config = context.obj
    project = sample.split('_')[0]
    output_path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance')

    # check if gt file exists
    gt_file = os.path.join(output_path, '{}.gt'.format(sample))
    if not os.path.exists(gt_file):
        log.error('gt file does not exist! Path: {}'.format(gt_file))
        log.error('To create .gt file run the command: gt_concordance parse_xl_files')
        return None

    # if we are here, the path has been already checked (most *likely*)
    if os.path.exists(output_path):
        # check if gatk needs to be run
        vcf_file = os.path.join(output_path, "{}.vcf".format(sample))
        to_run_gatk = False
        if os.path.exists(vcf_file):
            log.warning('.vcf file already exists: {}'.format(os.path.basename(vcf_file)))
            if force:
                log.info('Rerunning GATK because of --force option')
                to_run_gatk = True
            else:
                log.info('Skipping GATK')
        else:
            log.info('Running GATK')
            to_run_gatk = True

        # run gatk if needed
        if to_run_gatk:
            vcf_file = run_gatk(sample, config)
            if vcf_file is None:
                log.error('GATK completed with ERROR!')
                return None

        # check concordance
        vcf_data = parse_vcf_file(sample, config)
        gt_data = parse_gt_file(sample, config)
        if len(vcf_data) != len(gt_data):
            log.warning('VCF file and GT file contain different number of positions!! ({}, {})'.format(len(vcf_data), len(gt_data)))
        if len(vcf_data) <= 30:
            log.warning('VCF file contans only {} positions! Skipping concordance check'.format(len(vcf_data)))
            # generate .conc files, but update charon as 0
            check_concordance(sample, vcf_data, gt_data, config)
            concordance = 0.0
        else:
            concordance = check_concordance(sample, vcf_data, gt_data, config)
        return concordance

@click.pass_context
def is_config_file_ok(context):
    config = context.obj
    # check that required variables are present in config file
    ANALYSIS_PATH = config.get('ANALYSIS_PATH')
    if ANALYSIS_PATH is None:
        log.error("config file missing ANALYSIS_PATH")
        exit(1)
    if not os.path.exists(ANALYSIS_PATH):
        log.error('Analysis path does not exist! Path: {}'.format(ANALYSIS_PATH))
        exit(1)

    GATK_PATH = config.get('GATK_PATH')
    if GATK_PATH is None:
        log.error("config file missing GATK_PATH")
        exit(1)
    if not os.path.exists(GATK_PATH):
        log.error('GATK file does not exist! Path: {}'.format(GATK_PATH))
        exit(1)

    GATK_REF_FILE = config.get('GATK_REF_FILE')
    if GATK_REF_FILE is None:
        log.error("config file missing GATK_REF_FILE")
        exit(1)
    if not os.path.exists(GATK_REF_FILE):
        log.error('Reference file does not exist! Path: {}'.format(GATK_REF_FILE))
        exit(1)

    GATK_VAR_FILE = config.get('GATK_VAR_FILE')
    if GATK_VAR_FILE is None:
        log.error("config file missing GATK_VAR_FILE")
        exit(1)
    if not os.path.exists(GATK_VAR_FILE):
        log.error('GATK variant file does not exist! Path: {}'.format(GATK_VAR_FILE))
        exit(1)

    INTERVAL_FILE = config.get('INTERVAL_FILE')
    if INTERVAL_FILE is None:
        log.error('config file missing INTERVAL_FILE')
        exit(1)
    if not os.path.exists(INTERVAL_FILE):
        log.error('Interval file does not exist! Path: {}'.format(INTERVAL_FILE))
        exit(1)
    return True

def parse_vcf_file(sample, config):
    project = sample.split('_')[0]
    path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance', '{}.vcf'.format(sample))
    vcf_data = {}
    if os.path.exists(path):
        vcf_file = vcf.Reader(open(path, 'r'))
        for record in vcf_file:
            reference = str(record.REF[0])
            alternative = str(record.ALT[0])
            chromosome = str(record.CHROM)
            position = str(record.POS)

            genotype = str(record.genotype(sample)['GT'])
            a1, a2 = genotype.split('/')
            # 0 means no variant (using reference), 1 means variant (using alternative)
            a1 = reference if a1.strip() == '0' else alternative
            a2 = reference if a2.strip() == '0' else alternative
            vcf_data['{} {}'.format(chromosome, position)] = {
                'chromosome': chromosome,
                'position': position,
                'a1': a1,
                'a2': a2 }
    return vcf_data

def parse_gt_file(sample, config):
    project = sample.split('_')[0]
    path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance', '{}.gt'.format(sample))
    gt_data = {}
    if os.path.exists(path):
        with open(path, 'r') as gt_file:
            lines = gt_file.readlines()
            # skip first line (comment with xl filename)
            if lines[0].startswith('#'):
                lines = lines[1:]
            for line in lines:
                chromosome, position, rs_position, reference, alternative, a1, a2 = line.strip().split()
                gt_data['{} {}'.format(chromosome, position)] = {
                    'chromosome': chromosome,
                    'position': position,
                    'a1': a1,
                    'a2': a2 }
    return gt_data

def check_concordance(sample, vcf_data, gt_data, config):
    project = sample.split('_')[0]
    matches = []
    mismatches = []
    lost = []
    for chromosome_position in vcf_data.keys():
        chromosome, position = chromosome_position.split()
        vcf_a1 = vcf_data[chromosome_position]['a1']
        vcf_a2 = vcf_data[chromosome_position]['a2']
        if chromosome_position not in gt_data:
            log.warning('POSITION {} NOT FOUND IN GT DATA!!!'.format(chromosome_position))
            continue

        gt_a1 = gt_data[chromosome_position]['a1']
        gt_a2 = gt_data[chromosome_position]['a2']
        concordance = set([gt_a1, gt_a2]) == set([vcf_a1, vcf_a2])
        if concordance:
            matches.append([chromosome, position, vcf_a1, vcf_a2, gt_a1, gt_a2])
        else:
            if gt_a1 != '0' and gt_a2 != '0':
                mismatches.append([chromosome, position, vcf_a1, vcf_a2, gt_a1, gt_a2])
            else:
                lost.append([chromosome, position, vcf_a1, vcf_a2, gt_a1, gt_a2])

    # calculating concordance and round to 2 decimals
    if len(vcf_data) - len(lost)>0:
        percent_matches=round((float(len(matches))/float(len(vcf_data) - len(lost))*100), 2)
    else:
        percent_matches=0
    # sort by chromosome and position
    matches = sorted(matches, key=lambda x:(int(x[0]) if x[0] != 'X' else x[0], int(x[1])))
    mismatches = sorted(mismatches, key=lambda x:(int(x[0]) if x[0] != 'X' else x[0], int(x[1])))
    lost = sorted(lost, key=lambda x:(int(x[0]) if x[0] != 'X' else x[0], int(x[1])))

    # recording results
    result = '{}\n'.format(sample)
    result += 'Chrom Pos A1_seq A2_seq A1_maf A2_maf\n'
    result += 'Mismatches: {}\n'.format(len(mismatches))
    result += '\n'.join([' '.join(mismatch) for mismatch in mismatches])
    result += '\nLost: {}:\n'.format(len(lost))
    result += '\n'.join([' '.join(lost_snp) for lost_snp in lost])
    result += '\n'
    result += '\nLost snps: {}\n'.format(len(lost))
    result += 'Total number of matches: {} / {} / {}\n'.format(len(matches), len(vcf_data)-len(lost), len(vcf_data))
    result += 'Percent matches {}%\n'.format(percent_matches)


    # path should exist (if we came to this point), but checking anyway
    output_path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance')
    if os.path.exists(output_path):
        # create .conc file
        with open(os.path.join(output_path, '{}.conc'.format(sample)), 'w+') as conc_file:
            conc_file.write(result)

    # if numbers don't match, we return 0
    if len(matches) + len(mismatches) + len(lost) != len(vcf_data):
        log.warning('CHECK RESULTS!! Numbers are incoherent. Total number of positions: {}, matches: {}, mismatches: {}, lost: {}'.format(len(vcf_data), len(matches), len(mismatches), len(lost)))
        percent_matches = 0

    # if not enough positions, also return 0
    if len(matches) + len(mismatches) < 30:
        log.warning('Too few positions to calculate the concordance!')
        percent_matches = 0

    return percent_matches

def run_gatk(sample, config):
    project = sample.split('_')[0]
    ANALYSIS_PATH = config.get('ANALYSIS_PATH')
    # the path has been already checked, but checking again
    if os.path.exists(ANALYSIS_PATH):
        bamfile = os.path.join(ANALYSIS_PATH, project, 'piper_ngi/05_processed_alignments/{}.clean.dedup.bam'.format(sample))
        if not os.path.exists(bamfile):
            log.error('bamfile does not exist! {}'.format(bamfile))
            return None
        project = sample.split('_')[0]
        # the path has been already checked
        output_file = os.path.join(ANALYSIS_PATH, project, 'piper_ngi/03_genotype_concordance', "{sample}.vcf".format(sample=sample))
        options = """-T UnifiedGenotyper  -I {bamfile} -R {gatk_ref_file} -o {sample}  -D {gatk_var_file} -L {interval_file} -out_mode EMIT_ALL_SITES """.format(
                bamfile=bamfile,
                sample=output_file,
                interval_file=config.get('INTERVAL_FILE'),
                gatk_ref_file=config.get('GATK_REF_FILE'),
                gatk_var_file=config.get('GATK_VAR_FILE'))
        full_command = 'java -Xmx6g -jar {} {}'.format(config.get('GATK_PATH'), options)
        try:
            subprocess.call(full_command.split())
        except:
            return None

        return output_file

def update_gt_status_in_charon(sample_id, status, concordance=None):
    project_id = sample_id.split('_')[0]
    try:
        charon_session = CharonSession()
        sample = charon_session.sample_get(project_id, sample_id)
        if concordance is None:
            if sample.get('genotype_status') != status:
                charon_session.sample_update(projectid=project_id, sampleid=sample_id,genotype_status=status)
        else:
            if sample.get('genotype_status') != status or sample.get('genotype_concordance') != concordance:
                charon_session.sample_update(projectid=project_id, sampleid=sample_id,genotype_status=status, genotype_concordance=concordance)
    except CharonError as e:
        return str(e)

@cli.command()
@click.argument('project')
@click.option('--force', '-f', is_flag=True, default=False, help='If not specified, will keep existing vcf files and use them to check concordance. Otherwise overwrite')
@click.pass_context
def genotype_project(context, project, force):
    config = context.obj
    if is_config_file_ok():
        output_path = os.path.join(config.get('ANALYSIS_PATH'), project, 'piper_ngi/03_genotype_concordance')
        if not os.path.exists(output_path):
            log.error('Path does not exist! {}'.format(output_path))
            exit(1)
        list_of_gt_files = [file for file in os.listdir(output_path) if '.gt' in file]
        if not list_of_gt_files:
            log.error('No .gt files found in {}'.format(output_path))
            log.error('Generate .gt files first! Run the command: gt_concordance parse_xl_files')
            exit(1)
        log.info('{} .gt files found in {}'.format(len(list_of_gt_files), output_path))

        # genotype sample for each found gt_file
        results = {}
        failed = []
        for gt_file in list_of_gt_files:
            sample = gt_file.split('.')[0]
            concordance = run_genotype_sample(sample, force)
            if concordance is None:
                error = update_gt_status_in_charon(sample, status='FAILED')
                if error:
                    log.error('Sample has not been updated in Charon: {}'.format(sample))
                    log.error('Error says: {}'.format(error))
                failed.append(sample)
            else:
                error = update_gt_status_in_charon(sample, status='DONE', concordance=concordance)
                if error:
                    log.error('Sample has not been updated in Charon: {}'.format(sample))
                    log.error('Error says: {}'.format(error))
                results[sample] = concordance

        # print results
        if results:
            print('Sample name % concordance')
            for sample, concordance in sorted(list(results.items()), key=lambda s:s[0]):
                print('{} {}'.format(sample, concordance))
        # print failed
        if failed:
            print('Failed to check concordance for samples: ')
            for sample in failed:
                print(sample)

@cli.command()
@click.argument('project')
@click.option('--threshold', '-t', default=99, help='Threshold for concordance. Will print samples below this value', type=float)
@click.option('--all', '-a', 'all_samples', default=False, is_flag=True, help='If specified, will print ALL samples, both below and above the threshold')
@click.pass_context
def fetch_charon(context, project, threshold, all_samples):
    """
    Will fetch samples of the specified project from Charon and print the concordance
    """
    try:
    # get result from charon
        charon_session = CharonSession()
        result = charon_session.project_get_samples(project)
        samples = {}
        for sample in result.get('samples'):
            sample_id = sample.get('sampleid')
            concordance = float(sample.get('genotype_concordance'))
            status = sample.get('genotype_status')
            # exclude samples which were not yet checked
            if status is not None:
                samples[sample_id] = (concordance, status)

        # print output
        if not all_samples and samples:
            print('Samples below threshold: {}%'.format(threshold))
        for sample in sorted(samples.keys()):
            concordance, status = samples[sample]
            # if --all, we don't care about threshold
            if all_samples or concordance <= threshold:
                # do not print 0%
                if concordance != 0:
                    print('{} {}% {}'.format(sample, concordance, status))
    except Exception as e:
        log.error("Can't fetch Charon. Error says: {}".format(str(e)))


if __name__ == '__main__':
    cli()
