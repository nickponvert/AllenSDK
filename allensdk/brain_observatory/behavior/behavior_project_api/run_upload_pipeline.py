import os
import sys
import pandas as pd
sys.path.append('/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/src/pbstools')
from pbstools import PythonJob 

python_file = '/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/src/AllenSDK/allensdk/brain_observatory/behavior/behavior_project_api/upload_pipeline.py'
jobdir = '/allen/programs/braintv/workgroups/nc-ophys/nick.ponvert/cluster_jobs/20191008_db_upload'

job_settings = {'queue': 'braintv',
                'mem': '15g',
                'walltime': '4:00:00',
                'ppn':1,
                'jobdir': jobdir,
                }

from allensdk.brain_observatory.behavior import behavior_project_cache as bpc
manifest_path = "/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SFN_2019/cache_with_extra_inhibitory/behavior_project_manifest.json"
cache = bpc.InternalCacheFromLims(manifest=manifest_path)
sessions = cache.get_sessions()

# It is important that this dataframe of sessions is the same between this runner and the upload script. We should fix this to be independent.
scientifica_sessions = sessions[(sessions['equipment_name']!='MESO.1')&(~pd.isnull(sessions['stage_name']))]

for ind_session in range(len(scientifica_sessions)):
    PythonJob(
        python_file,
        python_executable='/home/nick.ponvert/anaconda3/envs/allen/bin/python',
        python_args=ind_session,
        conda_env=None,
        jobname = 'upload_session_{}'.format(ind_session),
        **job_settings
    ).run(dryrun=False)
