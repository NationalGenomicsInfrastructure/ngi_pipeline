import unittest
import mock

from ngi_pipeline.utils.communication import mail, mail_analysis


class TestCommunication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_name = "S.One_15_01"
        cls.sample_name = "P1155_101"
        cls.engine_name = "piper_ngi"
        cls.workflow = "Some Workflow"

    @mock.patch("ngi_pipeline.utils.communication.mail")
    @mock.patch("ngi_pipeline.utils.communication.traceback.extract_stack")
    def test_mail_analysis_info(self, mock_trace, mock_mail):
        """Send info email"""
        mock_trace.return_value = [
            ("some_file.py", 42, "<module>", "some_function()"),
            (),
        ]
        mail_analysis(
            project_name=self.project_name,
            sample_name=self.sample_name,
            engine_name=self.engine_name,
            level="INFO",
            info_text="Some information",
            workflow=self.workflow,
            config_file_path="data/test_ngi_config_minimal.yaml",
        )
        mock_mail.assert_called_once_with(
            origin="ngi_pipeline@scilifelab.se",
            recipient="some_user@some_email.com",
            subject="[S.One_15_01] [Some Workflow] [INFO] analysis information / status update",
            text=(
                "Get a load of this:\n"
                "Project: S.One_15_01\n"
                "Sample: P1155_101\n"
                "Engine: piper_ngi\n"
                "Workflow: Some Workflow\n"
                "File: some_file.py\n"
                "Line: 42\n\n"
                "Additional information:\n\n"
                "Some information\n"
            ),
        )

    @mock.patch("ngi_pipeline.utils.communication.mail")
    @mock.patch("ngi_pipeline.utils.communication.traceback.extract_stack")
    def test_mail_analysis_warn(self, mock_trace, mock_mail):
        """Send warning email"""
        mock_trace.return_value = [
            ("some_file.py", 42, "<module>", "some_function()"),
            (),
        ]
        mail_analysis(
            project_name=self.project_name,
            sample_name=self.sample_name,
            engine_name=self.engine_name,
            level="WARN",
            info_text="Warning: some warning",
            workflow=self.workflow,
            config_file_path="data/test_ngi_config_minimal.yaml",
        )
        mock_mail.assert_called_once_with(
            origin="ngi_pipeline@scilifelab.se",
            recipient="some_user@some_email.com",
            subject="[S.One_15_01] [Some Workflow] [WARN] analysis intervention may be needed",
            text=(
                "This analysis has produced a warning:\n"
                "Project: S.One_15_01\n"
                "Sample: P1155_101\n"
                "Engine: piper_ngi\n"
                "Workflow: Some Workflow\n"
                "File: some_file.py\n"
                "Line: 42\n\n"
                "Additional information:\n\n"
                "Warning: some warning\n"
            ),
        )

    @mock.patch("ngi_pipeline.utils.communication.mail")
    @mock.patch("ngi_pipeline.utils.communication.traceback.extract_stack")
    def test_mail_analysis_error(self, mock_trace, mock_mail):
        """Send error email"""
        mock_trace.return_value = [
            ("some_file.py", 42, "<module>", "some_function()"),
            (),
        ]
        mail_analysis(
            project_name=self.project_name,
            sample_name=self.sample_name,
            engine_name=self.engine_name,
            level="ERROR",
            info_text="Error: some error",
            workflow=self.workflow,
            config_file_path="data/test_ngi_config_minimal.yaml",
        )
        mock_mail.assert_called_once_with(
            origin="ngi_pipeline@scilifelab.se",
            recipient="some_user@some_email.com",
            subject="[S.One_15_01] [Some Workflow] [ERROR] analysis intervention required",
            text=(
                "This analysis has encountered an error:\n"
                "Project: S.One_15_01\n"
                "Sample: P1155_101\n"
                "Engine: piper_ngi\n"
                "Workflow: Some Workflow\n"
                "File: some_file.py\n"
                "Line: 42\n\n"
                "Additional information:\n\n"
                "Error: some error\n"
            ),
        )

    @mock.patch("ngi_pipeline.utils.communication.smtplib.SMTP")
    def test_mail(self, mock_SMTP):
        recipient = "some_user@some_email.com"
        subject = "Hello!"
        text = "Is it me you are looking for?"
        mail(recipient, subject, text, origin="sender@email.com")
        message = (
            'Content-Type: text/plain; charset="us-ascii"\n'
            "MIME-Version: 1.0\n"
            "Content-Transfer-Encoding: 7bit\n"
            "Subject: [NGI_pipeline] Hello!\n"
            "From: sender@email.com\n"
            "To: some_user@some_email.com\n\n"
            "Is it me you are looking for?"
        )
        mock_SMTP().sendmail.assert_called_once_with(
            "funk_002@nestor1.uppmax.uu.se", recipient, message
        )
