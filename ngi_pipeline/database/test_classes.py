import json
import requests
import unittest

from ngi_pipeline.database.classes import CharonSession, CHARON_BASE_URL

class TestCharonFunctions(unittest.TestCase):

    def setUp(self):
        self.session = CharonSession()
        self.p_id = "P100000"
        self.p_name = "Y.Mom_14_01"

    def test_construct_charon_url(self):
        append_list = ["road","to","nowhere"]
        # This is a weird test because it's the same code as I'm testing but it also seems weird to code it worse
        finished_url = "{}/api/v1/{}".format(CHARON_BASE_URL,'/'.join([str(a) for a in append_list]))
        # The method expects not a list but individual args
        self.assertEqual(finished_url, CharonSession().construct_charon_url(*append_list))

    def test_validate_response_wrapper(self):
        session = self.session

        # 400 Invalid input data
        data = {"malformed": "data"}
        with self.assertRaises(ValueError):
            session.post(session.construct_charon_url("project"),
                         data=json.dumps(data))

        # 404 Object not found
        p_id = "P000"
        with self.assertRaises(ValueError):
            session.get(session.construct_charon_url("project", p_id))

        # 405 Method not allowed
        with self.assertRaises(RuntimeError):
            # Should be GET
            session.post(session.construct_charon_url("projects"))

        # 409 Document revision conflict
        ## not sure how to fake this one

    def test_project_create_update_delete(self):
        self.session.project_create(self.p_id)
        self.session.project_update(proj_id=self.p_id, name=self.p_name)
        self.session.project_delete(proj_id=self.p_id)

    def projects_get_all(self):
        self.projects_get_all()
