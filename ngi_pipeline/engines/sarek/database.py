import contextlib

from ngi_pipeline.database.classes import CharonError, CharonSession
from ngi_pipeline.engines.sarek.process import (
    ProcessRunning,
    ProcessExitStatusSuccessful,
    ProcessExitStatusFailed,
    ProcessExitStatusUnknown,
    ProcessConnector,
    SlurmConnector,
)
from ngi_pipeline.engines.piper_ngi.database import get_db_session, SampleAnalysis
from ngi_pipeline.engines.sarek.exceptions import (
    AnalysisPipelineNotSpecifiedError,
    DatabaseProjectException,
    BestPracticeAnalysisNotSpecifiedError,
    SampleLookupError,
    AnalysisStatusForProcessStatusNotFoundError,
    SampleAnalysisStatusNotFoundError,
    SampleAnalysisStatusNotSetError,
    AlignmentStatusForAnalysisStatusNotFoundError,
    SampleUpdateError,
    SeqrunUpdateError,
    AnalysisReferenceNotSpecifiedError,
)


class CharonConnector(object):
    """
    The CharonConnector class provides an interface to the Charon database. Underneath, it uses the
    ngi_pipeline.database.classes.CharonSession class. Connection credentials are picked up from environment variables.
    """

    # mapping between a process status and the corresponding analysis status to record in Charon
    _ANALYSIS_STATUS_FROM_PROCESS_STATUS = {
        ProcessRunning: "UNDER_ANALYSIS",
        ProcessExitStatusSuccessful: "ANALYZED",
        ProcessExitStatusFailed: "FAILED",
        ProcessExitStatusUnknown: "FAILED",
    }

    # mapping between an analysis status and the corresponding alignment status to record in Charon
    _ALIGNMENT_STATUS_FROM_ANALYSIS_STATUS = {
        "TO_ANALYZE": "NOT_RUNNING",
        "UNDER_ANALYSIS": "RUNNING",
        "ANALYZED": "DONE",
        "FAILED": "FAILED",
    }

    def __init__(self, config, log, charon_session=None):
        """
        Create a CharonConnector object which provides an interface to the Charon sample tracking database.

        :param config: dict with configuration options
        :param log: a log handle where the connector will log its output
        :param charon_session: an active database session to use, if not specified, a new session will be created
        """
        self.config = config
        self.log = log
        self.charon_session = charon_session or CharonSession(config=self.config)

    def _fetch_project_field(self, projectid, field_key, exception_type=None):
        try:
            return self.charon_session.project_get(projectid)[field_key]
        except (KeyError, CharonError) as e:
            exception_type = exception_type or DatabaseProjectException
            exception = exception_type(projectid, reason=e)
            self.log.error(exception)
            raise exception

    def analysis_pipeline(self, projectid):
        return self._fetch_project_field(
            projectid, "pipeline", AnalysisPipelineNotSpecifiedError
        )

    def analysis_reference(self, projectid):
        return self._fetch_project_field(
            projectid, "reference", AnalysisReferenceNotSpecifiedError
        )

    def best_practice_analysis(self, projectid):
        return self._fetch_project_field(
            projectid, "best_practice_analysis", BestPracticeAnalysisNotSpecifiedError
        )

    def sample_analysis_status(self, projectid, sampleid):
        try:
            return self.charon_session.sample_get(projectid, sampleid)[
                "analysis_status"
            ]
        except (KeyError, CharonError) as e:
            analysis_status_exception = SampleAnalysisStatusNotFoundError(
                projectid, sampleid, reason=e
            )
            self.log.error(analysis_status_exception)
            raise analysis_status_exception

    def libprep_qc_status(self, projectid, sampleid, libprepid):
        try:
            return self.charon_session.libprep_get(projectid, sampleid, libprepid)["qc"]
        except (KeyError, CharonError) as e:
            sample_exception = SampleLookupError(
                projectid,
                sampleid,
                reason="qc status for libprep '{}' could not be fetched: {}".format(
                    libprepid, e
                ),
            )
            self.log.error(sample_exception)
            raise sample_exception

    def seqrun_alignment_status(self, projectid, sampleid, libprepid, seqrunid):
        try:
            return self.charon_session.seqrun_get(
                projectid, sampleid, libprepid, seqrunid
            )["alignment_status"]
        except (KeyError, CharonError) as e:
            sample_exception = SampleLookupError(
                projectid,
                sampleid,
                reason="alignment status for libprep '{}' and seqrun '{}' could not be fetched: {}".format(
                    libprepid, seqrunid, e
                ),
            )
            self.log.error(sample_exception)
            raise sample_exception

    def set_sample_analysis_status(self, status, *args, **kwargs):
        """
        Set the analysis status on a sample to the specified string. If recurse is True, the alignment status on
        the affected seqruns will be set as well (to the string corresponding to the analysis status according to
        the CharonConnector._ALIGNMENT_STATUS_FROM_ANALYSIS_STATUS mapping. See `set_sample_attribute` for restricting
        recursion.

        :param status: the analysis status to set as a string
        :param args: additional arguments that will be passed to `set_sample_attribute`
        :param kwargs: additional keyword arguments that will be passed to `set_sample_attribute`
        :return: a response object
        """
        try:
            kwargs["sample_update_kwargs"] = {"analysis_status": status}
            kwargs["seqrun_update_kwargs"] = {
                "alignment_status": self.alignment_status_from_analysis_status(status)
            }
            return self.set_sample_attribute(*args, **kwargs)
        except (SampleUpdateError, SeqrunUpdateError) as e:
            analysis_status_exception = SampleAnalysisStatusNotSetError(
                e.projectid, e.sampleid, status, reason=e
            )
            self.log.error(analysis_status_exception)
            raise analysis_status_exception

    def set_sample_duplication(self, pct_duplication, *args, **kwargs):
        return self._set_sample_metric(
            *args, sample_update_kwargs={"duplication_pc": pct_duplication}, **kwargs
        )

    def set_sample_autosomal_coverage(self, autosomal_coverage, *args, **kwargs):
        return self._set_sample_metric(
            *args,
            sample_update_kwargs={"total_autosomal_coverage": autosomal_coverage},
            **kwargs,
        )

    def set_sample_total_reads(self, total_reads, *args, **kwargs):
        return self._set_sample_metric(
            *args, sample_update_kwargs={"total_sequenced_reads": total_reads}, **kwargs
        )

    def _set_sample_metric(self, *args, **kwargs):
        try:
            return self.set_sample_attribute(*args, **kwargs)
        except SampleUpdateError as e:
            self.log.error(e)
            raise

    def set_sample_attribute(
        self,
        projectid,
        sampleid,
        sample_update_kwargs,
        seqrun_update_kwargs=None,
        recurse=False,
        restrict_to_libpreps=None,
        restrict_to_seqruns=None,
    ):
        """
        Update a sample according to the `sample_update_kwargs` dict. If recurse is True, the affected seqruns will be
        updated according to the `seqrun_update_kwargs` dict as well. Optionally, the libpreps and seqruns to
        recurse into can be restricted.

        :param projectid: the Charon projectid of the project to update
        :param sampleid: the Charon sampleid of the sample to update
        :param sample_update_kwargs: a dict to pass as keyword arguments to the
        `ngi_pipeline.database.classes.CharonSession.sample_update` method
        :param seqrun_update_kwargs: a dict to pass as keyword arguments to the
        `ngi_pipeline.database.classes.CharonSession.seqrun_update` method
        :param recurse: if True, the seqruns belonging to the sample will also be updated (default is False)
        :param restrict_to_libpreps: list with libpreps to restrict the recursion to. If specified, only seqruns
        belonging to libpreps in this list will be recursed into. Default is to iterate over seqruns in all libpreps
        :param restrict_to_seqruns: dict with libprepids as keys and a list with seqrunids as values. If specified,
        the seqruns to update for a libprepid will be restricted to the seqruns in the list. Default is to update
        all seqruns for the libpreps iterated over
        :return: a response object
        """
        try:
            if recurse:
                # iterate over all libpreps, taking the restrict_to_libpreps argument into account
                for libprep in self.sample_libpreps(
                    projectid, sampleid, restrict_to=restrict_to_libpreps
                ):
                    libprepid = libprep["libprepid"]
                    # iterate over the seqruns for the libprep and restrict to the specified seqruns if the libprep is
                    # a key in the restrict_to_seqruns dict
                    for seqrun in self.libprep_seqruns(
                        projectid,
                        sampleid,
                        libprepid,
                        restrict_to=restrict_to_seqruns.get(libprepid)
                        if restrict_to_seqruns
                        else None,
                    ):
                        try:
                            # set the alignment status on the seqrun according to the mapping
                            self.charon_session.seqrun_update(
                                projectid,
                                sampleid,
                                libprepid,
                                seqrun["seqrunid"],
                                **seqrun_update_kwargs,
                            )
                        except CharonError as e:
                            raise SeqrunUpdateError(
                                projectid,
                                sampleid,
                                libprepid,
                                seqrun["seqrunid"],
                                reason=e,
                            )
            # lastly, update the analysis status of the sample
            return self.charon_session.sample_update(
                projectid, sampleid, **sample_update_kwargs
            )
        except CharonError as e:
            raise SampleUpdateError(projectid, sampleid, reason=e)

    def sample_libpreps(self, projectid, sampleid, restrict_to=None):
        """
        Get all libpreps for a sample, optionally filtered by libprepid

        :param projectid: the project to get libpreps for
        :param sampleid: the sample to get libpreps for
        :param restrict_to: list with libprepids. If specified, only libpreps whose id is in the list will be returned
        :return: list of libpreps, represented as dicts, belonging to the specified sample
        """
        try:
            return [
                x
                for x in self.charon_session.sample_get_libpreps(projectid, sampleid)[
                    "libpreps"
                ]
                if restrict_to is None or x["libprepid"] in restrict_to
            ]
        except (KeyError, CharonError) as e:
            sample_libpreps_exception = SampleLookupError(projectid, sampleid, reason=e)
            self.log.error(sample_libpreps_exception)
            raise sample_libpreps_exception

    def libprep_seqruns(self, projectid, sampleid, libprepid, restrict_to=None):
        """
        Get all seqruns for a libprep, optionally filtered by seqrunid

        :param projectid: the project to get seqruns for
        :param sampleid: the sample to get seqruns for
        :param libprepid: the libprep to get seqruns for
        :param restrict_to: list with seqrunids. If specified, only seqruns whose id is in the list will be returned
        :return: list of seqruns, represented as dicts, belonging to the specified libprep
        """
        try:
            return [
                x
                for x in self.charon_session.libprep_get_seqruns(
                    projectid, sampleid, libprepid
                )["seqruns"]
                if restrict_to is None or x["seqrunid"] in restrict_to
            ]
        except (KeyError, CharonError) as e:
            libprep_seqruns_exception = SampleLookupError(projectid, sampleid, reason=e)
            self.log.error(libprep_seqruns_exception)
            raise libprep_seqruns_exception

    def analysis_status_from_process_status(self, process_status):
        """
        Translate a ProcessStatus to the corresponding status string used for the analysis status in Charon

        :param process_status: the process status class to translate
        :return: the corresponding string used in Charon
        :raises: AnalysisStatusForProcessStatusNotFoundError if the process status could not be translated
        """
        try:
            return self._ANALYSIS_STATUS_FROM_PROCESS_STATUS[process_status]
        except KeyError:
            charon_status_error = AnalysisStatusForProcessStatusNotFoundError(
                process_status
            )
            self.log.error(charon_status_error)
            raise charon_status_error

    def alignment_status_from_analysis_status(self, analysis_status):
        """
        Translate a sample analysis status used in Charon to the corresponding alignment status string used for
        seqruns in Charon

        :param analysis_status: the analysis status string to translate
        :return: the corresponding alignment status string used in Charon
        :raises: AlignmentStatusForAnalysisStatusNotFoundError if the process status could not be translated
        """
        try:
            return self._ALIGNMENT_STATUS_FROM_ANALYSIS_STATUS[analysis_status]
        except KeyError:
            status_error = AlignmentStatusForAnalysisStatusNotFoundError(
                analysis_status
            )
            self.log.error(status_error)
            raise status_error


class TrackingConnector(object):
    """
    The TrackingConnector class provides an interface to the local SQLite tracking database. Underneath, it uses some
    of the code from ngi_pipeline.engines.piper_ngi.database
    """

    # mapping between process connector types and the corresponding db field storing the job identifier
    PIDFIELD_FROM_PROCESS_CONNECTOR_TYPE = {
        ProcessConnector: "process_id",
        SlurmConnector: "slurm_job_id",
    }

    def __init__(self, config, log, tracking_session=None):
        """
        Create a TrackingConnector instance that provides an interface to the local tracking database.

        :param config: dict with configuration options
        :param log: a log handle where the connector will log its output
        :param tracking_session: a database session to use for the connection. If not specified, a new session will
        be created as necessary
        """
        self.config = config
        self.log = log
        self.tracking_session = tracking_session

    class _SampleAnalysis(SampleAnalysis):
        """
        Subclassing the SampleAnalysis model from ngi_pipeline.engines.piper_ngi.database so that we can override
        stuff if necessary
        """

        pass

    @staticmethod
    def pidfield_from_process_connector_type(process_connector_type):
        return TrackingConnector.PIDFIELD_FROM_PROCESS_CONNECTOR_TYPE[
            process_connector_type
        ]

    @contextlib.contextmanager
    def db_session(self):
        """
        Context manager for the database session
        :return: a database session
        """
        if self.tracking_session is not None:
            yield self.tracking_session
        else:
            with get_db_session(config=self.config) as db_session:
                self.tracking_session = db_session
                yield self.tracking_session

    def record_process_sample(
        self,
        projectid,
        sampleid,
        project_base_path,
        analysis_type,
        engine,
        pid,
        process_connector_type,
    ):
        """
        Add the processing details for a sample as a record in the tracking database. The database model is defined
        by the _SampleAnalysis class.

        :param projectid: project id for the sample
        :param sampleid: sample id for the sample
        :param project_base_path: path to the project base
        :param analysis_type: the name of the analysis instance class (e.g. SarekAnalysisGermline)
        :param engine: the name of the analysis engine (e.g. sarek)
        :param pid: the process or job id for the analysis
        :param process_connector_type: the type of the process connector used to start the analysis
        """
        # different database fields are used to record the process id depending on if it's a slurm job or a local job,
        # therefore we'll map the process connector type to the corresponding name of the field
        pidfield = self.pidfield_from_process_connector_type(process_connector_type)
        db_obj = self._SampleAnalysis(
            project_id=projectid,
            project_name=projectid,
            sample_id=sampleid,
            project_base_path=project_base_path,
            workflow=analysis_type,
            engine=engine,
            **{pidfield: pid},
        )

        with self.db_session() as db_session:
            db_session.add(db_obj)
            db_session.commit()

    def remove_analysis(self, analysis):
        """
        Remove an analysis record from the database
        :param analysis: the analysis record, as an instance of SampleAnalysis, to remove from the database
        """
        with self.db_session() as db_session:
            db_session.delete(analysis)
            db_session.commit()

    def tracked_analyses(self):
        """
        :return: a generator of SampleAnalysis objects representing analyses having "sarek" as the analysis engine
        that are tracked in the local tracking database
        """
        with self.db_session() as db_session:
            for analysis in (
                db_session.query(self._SampleAnalysis)
                .filter(self._SampleAnalysis.engine == "sarek")
                .all()
            ):
                yield analysis
