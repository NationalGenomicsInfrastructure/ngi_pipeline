
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.filesystem import execute_command_line, safe_makedir

import os

def run_multiqc(base_path, project_id, project_name, wait=False):

    project_path=os.path.join(base_path, 'ANALYSIS', project_id)
    result_path=os.path.join(base_path, 'ANALYSIS', project_id, 'multiqc')
    safe_makedir(result_path)
    command=['multiqc', project_path, '-o', result_path, '-i', project_name, '-n', project_name, '-q', '-f']
    multiqc_stdout=''
    multiqc_stderr=''
    try:
        #if multiqc is already running, kill it first.
        ps_command=["ps", "ux"]
        pcs=subprocess.check_output(ps_command)
        for line in pcs.splitlines():
            if " ".join(command) in line :
                os.kill(int(line.split()[1]), 9)

        #then run multiqc
        handle=execute_command_line(command)
        if wait:
            (multiqc_stdout, multiqc_stderr)=handle.communicate()
            if multiqc_stdout or multiqc_stderr:
                combined_output="{}\n{}".format(multiqc_stdout, multiqc_stderr)
                raise Exception(combined_output)

    except:
        raise


