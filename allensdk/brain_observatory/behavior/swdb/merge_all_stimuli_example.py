import os
import pandas as pd
from allensdk.brain_observatory.behavior.swdb import behavior_project_cache as bpc
cache_base = '/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SWDB_2019'
cache = bpc.BehaviorProjectCache(cache_base)

ind = 33
oeid = cache.experiment_table.loc[ind]['ophys_experiment_id']
container_id = cache.experiment_table.loc[ind]['container_id']

all_stimuli_base = '/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SWDB_2019/exposure_number'
full_path = os.path.join(all_stimuli_base, 'stimulus_exposures_container_{}.h5'.format(container_id))
all_stimuli = pd.read_hdf(full_path, key='df')

session = cache.get_session(oeid)

all_stimuli_this_session = all_stimuli[all_stimuli['ophys_experiment_id']==oeid][[
    'stimulus_presentations_id',
    'exposure_number',
    #  'equipment_name',
    #  'ophys_name',
]]

merged_stimuli = session.stimulus_presentations.merge(
    all_stimuli_this_session,
    left_index=True,
    right_on='stimulus_presentations_id',
    how='inner'
).set_index(session.stimulus_presentations.index)

