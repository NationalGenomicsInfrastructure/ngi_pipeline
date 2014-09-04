import argparse
import time
import os

from ngi_pipeline.conductor.flowcell import process_demultiplexed_flowcell, process_demultiplexed_flowcells
from ngi_pipeline.conductor.launchers import trigger_sample_level_analysis
from ngi_pipeline.database.process_tracking import check_update_jobs_status


def main(demux_fcid_dir, test_step_1, restrict_to_projects=None, restrict_to_samples=None):
    if not test_step_1:
        process_demultiplexed_flowcell(demux_fcid_dir[0], restrict_to_projects, restrict_to_samples)
    elif test_step_1:
    
        demux_fcid_dir = "/proj/a2010002/INBOX/130701_SN7001298_0153_BH0JMGADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None) # this must start
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/140528_D00415_0049_BC423WACXX" # G.Grigelioniene_14_01
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/140702_D00415_0052_AC41A2ANXX" # M.Kaller_14_06
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/130611_SN7001298_0148_AH0CCVADXX/" #A.Wedell_13_03 sample P567_101
        process_demultiplexed_flowcell(demux_fcid_dir, None, None)
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/130612_D00134_0019_AH056WADXX/" # A.Wedell_13_03 sample P567_101
        process_demultiplexed_flowcell(demux_fcid_dir, None, None) # this must start
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/130627_D00134_0023_AH0JYUADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None) # this must start
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/130701_SN7001298_0152_AH0J92ADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None) # this must start
        time.sleep(30) #wait for 1 minutes
        
        demux_fcid_dir = "/proj/a2010002/INBOX/130701_SN7001298_0153_BH0JMGADXX/" # A.Wedell_13_03 sample P567_102
        process_demultiplexed_flowcell(demux_fcid_dir, None, None) # this must start
        time.sleep(30) #wait for 1 minutes
        
        
        check_update_jobs_status()
        trigger_sample_level_analysis()
        
        for demux_fcid in ("146362_3YUPS4_4614_AYSD4DN8PK", "382000_FA15AC_1819_ADT0SZ8CAV", "526414_H4VQMR_0554_A2Z2D30IMP", "636430_FHTLAG_5491_ADVN76RAX0", "694405_SGXBZU_8447_ANAY949DA1", "707581_O81L8Q_5350_ABBQEYRQQR", "958288_QH3TH9_2625_AOET9TT9AK", "992290_KV8699_9429_AJM8N4NV1U"):
            demux_fcid_dir = os.path.join("/proj/a2010002/INBOX/", demux_fcid)
            process_demultiplexed_flowcell(demux_fcid_dir, None, None)
            time.sleep(30)
        
        
        #and now a loop to update the DB
        while True:
            check_update_jobs_status()
            trigger_sample_level_analysis()
            #check status every half an hour
            time.sleep(3800)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Quick launcher for testing purposes.")
    parser.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict processing to these projects. "
                  "Use flag multiple times for multiple projects.")) #            default=["G.Grigelioniene_14_01"],
    parser.add_argument("-s", "--sample", dest= "restrict_to_samples", action="append",
            help=("Restrict processing to these samples. "
                  "Use flag multiple times for multiple projects."))
    parser.add_argument("demux_fcid_dir", nargs="*", action="store",
            default="/proj/a2010002/nobackup/mario/DATA/140528_D00415_0049_BC423WACXX/",
            help=("The path to the Illumina demultiplexed fc directories "
                  "to process."))
    parser.add_argument("-t1", "--test_step_1", dest= "test_step_1",  action='store_true', default=False,
            help=("Simulation of pipeline behaviour using A.Wedell_13_03, M.Kaller_14_06, and G.Grigelione..."))
    args_dict = vars(parser.parse_args())
    main(**args_dict)
"""
project A.Wedell_13_03 is well suited for testing
A.Wedell_13_03
/proj/a2010002/INBOX/130611_SN7001298_0148_AH0CCVADXX/    --> P567_101
/proj/a2010002/INBOX/130612_D00134_0019_AH056WADXX/       --> P567_101
/proj/a2010002/INBOX/130627_D00134_0023_AH0JYUADXX/       --> P567_102
/proj/a2010002/INBOX/130701_SN7001298_0152_AH0J92ADXX/    --> P567_102
/proj/a2010002/INBOX/130701_SN7001298_0153_BH0JMGADXX/    --> P567_102
G.Grigelioniene_14_01
/proj/a2010002/INBOX/140528_D00415_0049_BC423WACXX        --> P1142_101
M.Kaller_14_06
/proj/a2010002/INBOX/140702_D00415_0052_AC41A2ANXX        --> P1171_1


"""
