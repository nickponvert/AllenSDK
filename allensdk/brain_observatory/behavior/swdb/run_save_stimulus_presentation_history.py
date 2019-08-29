import os
import sys
sys.path.append('/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/src/pbstools')
from pbstools import PythonJob 
import behavior_project_cache as bpc

python_file = r"/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/src/AllenSDK/allensdk/brain_observatory/behavior/swdb/save_stimulus_presentation_history.py"
jobdir = '/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/cluster_jobs/20190810_save_stimulus_presentation_history'

job_settings = {'queue': 'braintv',
                'mem': '24g',
                'walltime': '0:30:00',
                'ppn':1,
                'jobdir': jobdir,
                }

cache_base = '/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SWDB_2019'
cache = bpc.BehaviorProjectCache(cache_base)
manifest_containers = cache.experiment_table['container_id'].unique()

for this_container_id in manifest_containers:
    PythonJob(
        python_file,
        python_executable = '/home/nick.ponvert/anaconda3/envs/allen/bin/python',
        python_args = this_container_id,
        conda_env = None,
        jobname = 'stimulus_presentation_history_{}'.format(this_container_id),
        **job_settings
    ).run(dryrun=False)
