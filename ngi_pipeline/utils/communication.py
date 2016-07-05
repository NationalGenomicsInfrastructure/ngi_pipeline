import smtplib
import traceback

from email.mime.text import MIMEText

from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)

def mail(recipient, subject, text, origin="seqmaster@scilifelab.se"):
    msg = MIMEText(text)
    msg['Subject'] = '[NGI_pipeline] {}'.format(subject)
    msg['From'] = origin
    msg['To'] = recipient
    s = smtplib.SMTP('localhost', 25)
    s.sendmail('funk_002@nestor1.uppmax.uu.se', recipient, msg.as_string()) 
    s.quit()

@with_ngi_config
def mail_analysis(project_name, sample_name=None, engine_name=None,
                  level="ERROR", info_text=None, workflow=None,
                  recipient="ngi_pipeline_operators@scilifelab.se",
                  subject=None, origin="seqmaster@scilifelab.se",
                  config=None, config_file_path=None ):

    # check for mail recipient among the config values
    # if it is not configured, we are defaulting to ngi_pipeline_operators@scilifelab.se
    try:
        recipient = config.get('mail')['recipient']
    except TypeError:
        LOG.info("no mail recipient is defined in config file " + config_file_path + "; defaulting to " + recipient)

    file_name, line_no = traceback.extract_stack(limit=2)[-2][:2]
    if level.upper() == "WARN":
        text = "This analysis has produced a warning:"
        if not subject:
            subject = "analysis intervention may be needed"
        subject = "[WARN] " + subject
    elif level.upper() == "INFO":
        text = "Get a load of this:"
        if not subject:
            subject = "analysis information / status update"
        subject = "[INFO] " + subject
    else: # ERROR
        text = "This analysis has encountered an error:"
        if not subject:
            subject = "analysis intervention required"
        subject = "[ERROR] " + subject
    if workflow:
        subject = "[{}] ".format(workflow) + subject
    subject = "[{}] ".format(project_name) + subject
    text += "\nProject: {}".format(project_name)
    if sample_name:
        text += "\nSample: {}".format(sample_name)
    if engine_name:
        text += "\nEngine: {}".format(engine_name)
    if workflow:
        text += "\nWorkflow: {}".format(workflow)
    text += "\nFile: {}".format(file_name)
    text += "\nLine: {}".format(line_no)
    if info_text:
        text = text + "\n\nAdditional information:\n\n{}\n".format(info_text)
    mail(recipient=recipient, subject=subject, text=text, origin=origin)
