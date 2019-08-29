import sys
import os
import pandas as pd
import numpy as np
from allensdk.internal.api import PostgresQueryMixin
from allensdk.brain_observatory.behavior.swdb import behavior_project_cache as bpc
from allensdk.brain_observatory.behavior import stimulus_processing
from importlib import reload; reload(stimulus_processing)
from visual_behavior.translator.foraging2 import data_to_change_detection_core
from tqdm import tqdm


postgres_api = PostgresQueryMixin()
all_behavior_sessions_query = '''
SELECT
 
vbc.id as container_id,
vbc.workflow_state as container_qc_state,
d.id as donor_id,
bs.id as behavior_session_id,
bs.created_at,
bs.foraging_id,
e.name as equipment_name,
os.id as ophys_session_id,
os.name as ophys_session_name,
oe.id as ophys_experiment_id,
oe.workflow_state as ophys_qc_state,
p.name as project_name,
imaging_depths.depth as imaging_depth,
st.acronym as targeted_structure
 
FROM visual_behavior_experiment_containers vbc
JOIN specimens sp ON sp.id=vbc.specimen_id
JOIN donors d ON d.id=sp.donor_id
LEFT JOIN behavior_sessions bs ON bs.donor_id = d.id
LEFT JOIN equipment e on e.id = bs.equipment_id
LEFT JOIN ophys_sessions os on os.foraging_id = bs.foraging_id
LEFT JOIN projects p ON p.id=os.project_id
LEFT JOIN ophys_experiments oe on oe.ophys_session_id = os.id
LEFT JOIN structures st ON st.id=oe.targeted_structure_id
LEFT JOIN imaging_depths ON imaging_depths.id=oe.imaging_depth_id
'''


def get_pickle_path(behavior_session_id):
    api = PostgresQueryMixin()
    query = '''
            SELECT stim.storage_directory || stim.filename AS stim_file
            FROM behavior_sessions bs

            LEFT JOIN well_known_files stim 
            ON stim.attachable_id=bs.id
            AND stim.attachable_type = 'BehaviorSession'
            AND stim.well_known_file_type_id
            IN (SELECT id
                FROM well_known_file_types
                WHERE name = 'StimulusPickle')
            WHERE bs.id = {}
            '''.format(behavior_session_id)
    return api.fetchone(query)


def get_stimulus_timestamps(data):
    # Stack intervalsms to act as stimulus timestamps
    vsyncs = data["items"]["behavior"]["intervalsms"]
    return np.hstack((0, vsyncs)).cumsum() / 1000.0

def ensure_has_omitted_flash_frame_log(data):
    if 'omitted_flash_frame_log' not in data['items']['behavior']:
        # We have to infer the omitted flashes
        core_data = data_to_change_detection_core(data)
        if len(core_data['omitted_stimuli'])>0:
            data['items']['behavior']['omitted_flash_frame_log'] = {
                'images_0': core_data['omitted_stimuli']['frame'].tolist()
            }
        else:
            data['items']['behavior']['omitted_flash_frame_log'] = {
                'images_0': []
            }
    return data

if __name__=="__main__":

    this_container_id = str(sys.argv[1])
    all_sessions = pd.read_sql(all_behavior_sessions_query, postgres_api.get_connection())

    sessions_this_container = all_sessions.query(
        'container_id == @this_container_id'
    ).sort_values('created_at')

    all_stimulus_dfs = []

    print("Reading stimulus pickle files")
    for ind_enum, ind_row in enumerate(tqdm(sessions_this_container.index)):
        bsid = sessions_this_container.at[ind_row, 'behavior_session_id']
        oeid = sessions_this_container.at[ind_row, 'ophys_experiment_id']
        equipment_name = sessions_this_container.at[ind_row, 'equipment_name']
        ophys_session_name = sessions_this_container.at[ind_row, 'ophys_session_name']
        oeid = sessions_this_container.at[ind_row, 'ophys_experiment_id']
        pickle_path = get_pickle_path(bsid)
        data = pd.read_pickle(pickle_path)
        if 'behavior' in data['items']:
            data = ensure_has_omitted_flash_frame_log(data)
            stimulus_timestamps = get_stimulus_timestamps(data)
            #  stimulus_presentations = stimulus_processing.get_visual_stimuli_df(data, stimulus_timestamps)
            stimulus_presentations = stimulus_processing.get_stimulus_presentations(data, stimulus_timestamps)
            stimulus_presentations['ind_session'] = ind_enum
            stimulus_presentations['date'] = sessions_this_container.at[ind_row, 'created_at']
            stimulus_presentations['ophys_experiment_id'] = oeid
            stimulus_presentations['behavior_session_id'] = bsid
            stimulus_presentations['equipment_name'] = equipment_name
            stimulus_presentations['ophys_name'] = ophys_session_name
            all_stimulus_dfs.append(stimulus_presentations)
        else:
            # This happens for RF mapping sessions
            continue

    #  all_stimuli = pd.concat(all_stimulus_dfs).reset_index().rename(
    #      columns={'index':'stimulus_presentations_id'}
    #  )
    all_stimuli = pd.concat(all_stimulus_dfs).reset_index()

    # Make an 'image_name' for oriented gratings
    print("Renaming oriented gratings")
    for ind_row in all_stimuli.index:
        if not pd.isnull(all_stimuli.at[ind_row, 'orientation']):
            if pd.isnull(all_stimuli.at[ind_row, 'image_name']):
                all_stimuli.at[ind_row, 'image_name'] = 'gratings_{}'.format(
                    all_stimuli.at[ind_row, 'orientation']
                )
            else:
                raise ValueError('non_null orientation and image_name')

    # Save exposure number for each image
    print("Calculating exposure number")
    for group_name, image_group in all_stimuli.groupby('image_name'):
        for ind_enum, ind_row in enumerate(image_group.index):
            all_stimuli.at[ind_row, 'exposure_number'] = ind_enum

    output_dir = '/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SWDB_2019/exposure_number'
    full_path = os.path.join(output_dir, 'stimulus_exposures_container_{}.h5'.format(this_container_id))
    all_stimuli.to_hdf(full_path, key='df')
