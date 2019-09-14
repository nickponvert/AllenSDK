import os
import pandas as pd
import numpy as np
from allensdk.api.cache import memoize
from allensdk.internal.api import PostgresQueryMixin
from allensdk.internal.api.behavior_ophys_api import BehaviorOphysLimsApi
from allensdk.brain_observatory.behavior.stimulus_processing import get_stimulus_presentations, get_stimulus_metadata

class PickleFileApi(BehaviorOphysLimsApi):
    def __init__(self, behavior_session_id):
        self.behavior_session_id = behavior_session_id
        
    @memoize
    def get_behavior_stimulus_file(self):
        api = PostgresQueryMixin()
        query = '''
                SELECT stim.storage_directory || stim.filename AS stim_file 
                FROM behavior_sessions bs 
                LEFT JOIN well_known_files stim 
                    ON stim.attachable_id=bs.id 
                    AND stim.attachable_type = 'BehaviorSession' 
                    AND stim.well_known_file_type_id IN (
                        SELECT id 
                        FROM well_known_file_types 
                        WHERE name = 'StimulusPickle'
                        ) 
                WHERE bs.id= {};
                '''.format(self.behavior_session_id)
        return api.fetchone(query, strict=True)

    @memoize
    def get_stimulus_timestamps(self):
        # We don't have a sync file, so we have to get vsync times from the pickle file
        behavior_stimulus_file = self.get_behavior_stimulus_file()
        data = pd.read_pickle(behavior_stimulus_file)
        vsyncs = data["items"]["behavior"]["intervalsms"]
        return np.hstack((0, vsyncs)).cumsum() / 1000.0  # cumulative time

    @memoize
    def get_licks(self):
        # Get licks from pickle file instead of sync
        behavior_stimulus_file = self.get_behavior_stimulus_file()
        data = pd.read_pickle(behavior_stimulus_file)
        stimulus_timestamps = self.get_stimulus_timestamps()
        lick_frames = data['items']['behavior']['lick_sensors'][0]['lick_events']
        lick_times = [stimulus_timestamps[frame] for frame in lick_frames]
        return pd.DataFrame({'timestamps': lick_times})
    
    def get_stimulus_rebase_function(self):
        return lambda x: x
    
    @memoize
    def _get_stimulus_presentations(self):
        stimulus_timestamps = self.get_stimulus_timestamps()
        behavior_stimulus_file = self.get_behavior_stimulus_file()
        data = pd.read_pickle(behavior_stimulus_file)
        stimulus_presentations_df_pre = get_stimulus_presentations(data, stimulus_timestamps)
        #stimulus_presentations = get_stimulus_presentations(data, stimulus_timestamps)
        if pd.isnull(stimulus_presentations_df_pre['image_name']).all():
            if ~pd.isnull(stimulus_presentations_df_pre['orientation']).all():
                stimulus_presentations_df_pre['image_name'] = stimulus_presentations_df_pre['image_name'].astype(str)
                for ind_row in stimulus_presentations_df_pre.index:
                    stimulus_presentations_df_pre.at[ind_row, 'image_name'] = 'gratings_{}'.format(
                        stimulus_presentations_df_pre.at[ind_row, 'orientation']
                    )
            else:
                raise ValueError('non_null orientation and image_name')
                    
        if 'images' in data["items"]["behavior"]["stimuli"]:
            stimulus_metadata_df = get_stimulus_metadata(data) 
        else:
            image_names = stimulus_presentations_df_pre['image_name'].unique()
            image_groups = image_names
            image_sets = ['vertical' if x in ['gratings_0', 'gratings_180'] else 'horizontal' for x in image_names]
            stimulus_metadata_df = pd.DataFrame({
                'image_name': image_names,
                'image_group': image_groups,
                'image_set': image_sets
            })
            stimulus_metadata_df.index.name = 'image_index'

        idx_name = stimulus_presentations_df_pre.index.name
        stimulus_index_df = stimulus_presentations_df_pre.reset_index().merge(stimulus_metadata_df.reset_index(), on=['image_name']).set_index(idx_name)
        stimulus_index_df.sort_index(inplace=True)
        stimulus_index_df = stimulus_index_df[['image_set', 'image_index', 'start_time']].rename(columns={'start_time': 'timestamps'})
        stimulus_index_df.set_index('timestamps', inplace=True, drop=True)
        stimulus_presentations_df = stimulus_presentations_df_pre.merge(stimulus_index_df, left_on='start_time', right_index=True, how='left')
        assert len(stimulus_presentations_df_pre) == len(stimulus_presentations_df)

        return stimulus_presentations_df[sorted(stimulus_presentations_df.columns)]
