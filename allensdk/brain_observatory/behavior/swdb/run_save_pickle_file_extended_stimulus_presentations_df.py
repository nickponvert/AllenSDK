import os
import sys
sys.path.append('/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/src/pbstools')
from pbstools import PythonJob 
import behavior_project_cache as bpc

python_file = r"/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/src/AllenSDK/allensdk/brain_observatory/behavior/swdb/save_pickle_file_extended_stimulus_presentations_df.py"

jobdir = '/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/cluster_jobs/20190910_pickle_file_extended_stim'

job_settings = {'queue': 'braintv',
                'mem': '15g',
                'walltime': '0:30:00',
                'ppn':1,
                'jobdir': jobdir,
                }

cache_base = "/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SWDB_2019/"
cache = bpc.BehaviorProjectCache(cache_base)

donor_ids = cache.experiment_table['animal_name'].unique()

for donor_id in donor_ids:

    behavior_session_ids = bpc.get_all_behavior_sessions(donor_id, exclude_imaging_sessions=True)['behavior_session_id'].values

    for behavior_session_id in behavior_session_ids:
        PythonJob(
            python_file,
            python_executable = '/home/nick.ponvert/anaconda3/envs/allen/bin/python',
            python_args = behavior_session_id,
            conda_env = None,
            jobname = 'extended_stimulus_df_{}'.format(behavior_session_id),
            **job_settings
        ).run(dryrun=False)
