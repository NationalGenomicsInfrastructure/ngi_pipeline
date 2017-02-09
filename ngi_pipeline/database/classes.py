from __future__ import print_function

import functools
import json
import os
import re
import requests

from ngi_pipeline.database.utils import load_charon_variables
from ngi_pipeline.log.loggers import minimal_logger
from requests.exceptions import Timeout

LOG = minimal_logger(__name__)

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CharonSession(requests.Session):
    # Yeah that's right, I'm using __metaclass__
    # I even looked up how to do it on StackOverflow all by myself
    __metaclass__ = Singleton
    def __init__(self, config=None, config_file_path=None):
        super(CharonSession, self).__init__()

        _charon_vars_dict = load_charon_variables(config=config,
                                                  config_file_path=config_file_path)
        try:
            self._api_token = _charon_vars_dict['charon_api_token']
            self._api_token_dict = {'X-Charon-API-token': self._api_token}
            # Remove trailing slashes
            m = re.match(r'(?P<url>.*\w+)/*', _charon_vars_dict['charon_base_url'])
            if m:
                _charon_vars_dict['charon_base_url'] = m.groups()[0]
            self._base_url = _charon_vars_dict['charon_base_url']
        except KeyError as e:
            raise ValueError('Unable to load needed Charon variable: {}'.format(e))

        self.get = validate_response(functools.partial(self.get,
                    headers=self._api_token_dict, timeout=3))
        self.post = validate_response(functools.partial(self.post,
                    headers=self._api_token_dict, timeout=3))
        self.put = validate_response(functools.partial(self.put,
                    headers=self._api_token_dict, timeout=3))
        self.delete = validate_response(functools.partial(self.delete,
                    headers=self._api_token_dict, timeout=3))

        self._project_params = ('projectid', 'name', 'status', 'best_practice_analysis',
                                'sequencing_facility', 'delivery_status')
        self._project_reset_params = tuple(set(self._project_params) - \
                                           set(['projectid', 'name',
                                                'best_practice_analysis',
                                                'sequencing_facility']))
        self._sample_params = ('sampleid', 'status', 'analysis_status', 'qc_status',
                               'genotype_status', 'genotype_concordance',
                               'total_autosomal_coverage', 'total_sequenced_reads',
                               'delivery_status', 'duplication_pc', 'type', 'pair')
        self._sample_reset_params = tuple(set(self._sample_params) - \
                                          set(['sampleid', 'total_sequenced_reads']))
        self._libprep_params = ('libprepid', 'qc')
        self._libprep_reset_params = tuple()
        self._seqrun_params = ('seqrunid', 'lane_sequencing_status',
                               'alignment_status', 'genotype_status',
                               'total_reads', 'mean_autosomal_coverage')
        self._seqrun_reset_params = tuple(set(self._seqrun_params) - \
                                          set(['seqrunid', 'lane_sequencing_status',
                                               'total_reads']))

    def construct_charon_url(self, *args):
        """Build a Charon URL, appending any *args passed."""
        return "{}/api/v1/{}".format(self._base_url,'/'.join([str(a) for a in args]))


    def reset_base_url(self, charon_url):
        LOG.info('Resetting Charon base URL from "{}" to "{}"'.format(self._base_url,
                                                                      charon_url))
        self._base_url = charon_url

    # Project
    def project_create(self, projectid, name=None, status=None,
                       best_practice_analysis=None, sequencing_facility=None):
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._project_params }
        return self.post(self.construct_charon_url('project'),
                         data=json.dumps(data)).json()

    def project_get(self, projectid):
        return self.get(self.construct_charon_url('project', projectid)).json()


    def project_get_samples(self, projectid):
        return self.get(self.construct_charon_url('samples', projectid)).json()

    def project_update(self, projectid, name=None, status=None, best_practice_analysis=None,
                       sequencing_facility=None, delivery_status=None):
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._project_params if l_dict.get(k)}
        return self.put(self.construct_charon_url('project', projectid),
                        data=json.dumps(data)).text

    def projects_get_all(self):
        return self.get(self.construct_charon_url('projects')).json()

    def project_reset(self, projectid):
        url = self.construct_charon_url("project", projectid)
        data = { k: None for k in self._project_reset_params}
        return self.put(url, json.dumps(data)).text

    def project_delete(self, projectid):
        return self.delete(self.construct_charon_url('project', projectid)).text

    # Sample
    def sample_create(self, projectid, sampleid, status=None, analysis_status=None,
                      qc_status=None, genotype_status=None,
                      genotype_concordance=None, total_autosomal_coverage=None,
                      total_sequenced_reads=None, delivery_status=None):
        url = self.construct_charon_url("sample", projectid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._sample_params }
        return self.post(url, json.dumps(data)).json()

    def sample_get(self, projectid, sampleid):
        url = self.construct_charon_url("sample", projectid, sampleid)
        return self.get(url).json()

    def sample_get_libpreps(self, projectid, sampleid):
        return self.get(self.construct_charon_url('libpreps', projectid, sampleid)).json()

    def sample_get_projects(self, sampleid):
        return self.get(self.construct_charon_url('projectidsfromsampleid', sampleid)).json()

    def sample_update(self, projectid, sampleid, status=None, analysis_status=None,
                      qc_status=None, genotype_status=None,
                      genotype_concordance=None, total_autosomal_coverage=None,
                      total_sequenced_reads=None, delivery_status=None, duplication_pc=None):
        url = self.construct_charon_url("sample", projectid, sampleid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._sample_params if l_dict.get(k)}
        return self.put(url, json.dumps(data)).text

    def sample_reset(self, projectid, sampleid):
        url = self.construct_charon_url("sample", projectid, sampleid)
        data = { k: None for k in self._sample_reset_params}
        return self.put(url, json.dumps(data)).text

    def sample_delete(self, projectid, sampleid):
        return self.delete(self.construct_charon_url("sample", projectid, sampleid))

    # LibPrep
    def libprep_create(self, projectid, sampleid, libprepid, qc=None):
        url = self.construct_charon_url("libprep", projectid, sampleid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._libprep_params }
        return self.post(url, json.dumps(data)).json()

    def libprep_get(self, projectid, sampleid, libprepid):
        url = self.construct_charon_url("libprep", projectid, sampleid, libprepid)
        return self.get(url).json()

    def libprep_get_seqruns(self, projectid, sampleid, libprepid):
        return self.get(self.construct_charon_url('seqruns', projectid, sampleid, libprepid)).json()

    def libprep_update(self, projectid, sampleid, libprepid, qc=None):
        url = self.construct_charon_url("libprep", projectid, sampleid, libprepid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._libprep_params if l_dict.get(k)}
        return self.put(url, json.dumps(data)).text

    def libprep_reset(self, projectid, sampleid, libprepid):
        url = self.construct_charon_url("libprep", projectid, sampleid, libprepid)
        data = { k: None for k in self._libprep_reset_params}
        return self.put(url, json.dumps(data)).text

    def libprep_delete(self, projectid, sampleid, libprepid):
        return self.delete(self.construct_charon_url("libprep", projectid, sampleid, libprepid))

    # SeqRun
    def seqrun_create(self, projectid, sampleid, libprepid, seqrunid,
                      lane_sequencing_status=None, alignment_status=None,
                      genotype_status=None, runid=None, total_reads=None,
                      mean_autosomal_coverage=None):
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid)
        l_dict = locals()
        data = { k: l_dict.get(k) for k in self._seqrun_params }
        return self.post(url, json.dumps(data)).json()

    def seqrun_get(self, projectid, sampleid, libprepid, seqrunid):
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid)
        return self.get(url).json()

    def seqrun_update(self, projectid, sampleid, libprepid, seqrunid,
                      lane_sequencing_status=None, alignment_status=None,
                      genotype_status=None, runid=None, total_reads=None,
                      mean_autosomal_coverage=None, *args, **kwargs):
        if args: LOG.debug("Ignoring extra args: {}".format(", ".join(*args)))
        if kwargs: LOG.debug("Ignoring extra kwargs: {}".format(", ".join(["{}: {}".format(k,v) for k,v in kwargs.iteritems()])))
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid)
        l_dict = locals()
        data = { k: str(l_dict.get(k)) for k in self._seqrun_params if l_dict.get(k)}
        return self.put(url, json.dumps(data)).text

    def seqrun_reset(self, projectid, sampleid, libprepid, seqrunid):
        url = self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid)
        data = { k: None for k in self._seqrun_reset_params}
        return self.put(url, json.dumps(data)).text

    def seqrun_delete(self, projectid, sampleid, libprepid, seqrunid):
        return self.delete(self.construct_charon_url("seqrun", projectid, sampleid, libprepid, seqrunid))


class CharonError(Exception):
    def __init__(self, message, status_code=None, *args, **kwargs):
        self.status_code = status_code
        super(CharonError, self).__init__(message, *args, **kwargs)


class validate_response(object):
    """
    Validate or raise an appropriate exception for a Charon API query.
    """
    def __init__(self, f):
        self.f = f
        ## Should these be class attributes? I don't really know
        self.SUCCESS_CODES = (200, 201, 204)
        # There are certainly more failure codes I need to add here
        self.FAILURE_CODES = {
                400: (CharonError, ("Charon access failure: invalid input "
                                    "data (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")),
                404: (CharonError, ("Charon access failure: not found "
                                    "in database (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")), # when else can we get this? malformed URL?
                405: (CharonError, ("Charon access failure: method not "
                                    "allowed (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")),
                408: (CharonError, ("Charon access failure: connection timed out")),
                409: (CharonError, ("Charon access failure: document "
                                    "revision conflict (reason '{response.reason}' / "
                                    "code {response.status_code} / "
                                    "url '{response.url}')")),}

    def __call__(self, *args, **kwargs):
        try:
            response = self.f(*args, **kwargs)
        except Timeout as e:
            c_e = CharonError(e)
            c_e.status_code = 408
            raise c_e
        if response.status_code not in self.SUCCESS_CODES:
            try:
                err_type, err_msg = self.FAILURE_CODES[response.status_code]
            except KeyError:
                # Error code undefined, used generic text
                err_type = CharonError
                err_msg = ("Charon access failure: {response.reason} "
                           "(code {response.status_code} / url '{response.url}')")
            raise err_type(err_msg.format(**locals()), response.status_code)
        return response
