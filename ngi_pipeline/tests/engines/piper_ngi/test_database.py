import os
import random
import sqlalchemy
import tempfile
import unittest

from ngi_pipeline.engines.piper_ngi import database as sql_db
from ngi_pipeline.tests import generate_test_data as gtd


class TestSqlAlchemyDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.mkdtemp()
        cls.database_path = os.path.join(cls.tmp_dir, "temporary_database")
        cls.session = sql_db.get_db_session(database_path=cls.database_path)
        cls.project_name = "Y.Mom_14_01"
        cls.project_id = "P123"
        cls.sample_id = "{}_456".format(cls.project_id)
        cls.libprep_id = "A"
        cls.seqrun_id = gtd.generate_run_id()
        cls.engine = "piper_ngi"
        cls.workflow = "your_mom_has_a_workflow"

    def setUp(self):
        self.process_id = random.randint(2, 65536)

    def test_add_sample_analysis(self):
        sample_analysis = sql_db.SampleAnalysis(
            project_id=self.project_id,
            sample_id=self.sample_id,
            workflow=self.workflow,
            process_id=self.process_id,
        )
        with self.session as session:
            session.add(sample_analysis)
            session.commit()

            query = (
                session.query(sql_db.SampleAnalysis)
                .filter_by(process_id=self.process_id)
                .one()
            )
        self.assertEqual(query, sample_analysis)
