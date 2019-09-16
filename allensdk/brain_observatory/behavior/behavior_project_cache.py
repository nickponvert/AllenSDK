import pandas as pd
from functools import partial
from allensdk.api.cache import Cache
from allensdk.internal.api import PostgresQueryMixin
from allensdk.brain_observatory.ecephys.ecephys_project_cache import call_caching
from allensdk.brain_observatory.ecephys.file_promise import write_from_stream
from allensdk.brain_observatory.behavior.behavior_project_api.behavior_project_fixed_api import BehaviorProjectFixedApi
from allensdk.brain_observatory.behavior.behavior_project_api.behavior_project_lims_api import BehaviorProjectLimsApi
from allensdk.brain_observatory.behavior.behavior_ophys_api.behavior_ophys_nwb_api import BehaviorOphysNwbApi, ExtendedBehaviorOphysNwbApi
from allensdk.brain_observatory.behavior.behavior_ophys_session import BehaviorOphysSession, ExtendedBehaviorOphysSession
from allensdk.internal.api.behavior_ophys_api import BehaviorOphysLimsApi
from allensdk.internal.api.behavior_pickle_file_api import PickleFileApi
from allensdk.brain_observatory.mesoscope import mesoscope_session as meso

csv_io = {
    'reader': lambda path: pd.read_csv(path, index_col='Unnamed: 0'),
    'writer': lambda path, df: df.to_csv(path)
}

class BehaviorProjectCache(Cache):

    SESSIONS_KEY = 'sessions'

    NWB_DIR_KEY = 'nwb_files'
    SESSION_NWB_KEY = 'session_nwb'

    ANALYSIS_FILES_DIR_KEY = 'analysis_files'
    TRIAL_RESPONSE_KEY = 'trial_response'
    FLASH_RESPONSE_KEY = 'flash_response'
    EXTENDED_STIM_COLUMNS_KEY = 'extended_stimulus_presentations'
    EXPOSURE_NUMBER_KEY = 'exposure_number'

    MANIFEST_VERSION = '0.0.1'

    def __init__(self, fetch_api, **kwargs):
        
        kwargs['manifest'] = kwargs.get('manifest', 'behavior_project_manifest.json')
        kwargs['version'] = kwargs.get('version', self.MANIFEST_VERSION)

        super(BehaviorProjectCache, self).__init__(**kwargs)
        self.fetch_api = fetch_api

    def get_sessions(self, **get_sessions_kwargs):
        path = self.get_cache_path(None, self.SESSIONS_KEY)
        return call_caching(partial(self.fetch_api.get_sessions, **get_sessions_kwargs),
                            path=path, strategy='lazy', writer=csv_io["writer"], reader=csv_io['reader'])

    def get_session_data(self, experiment_id):
        # TODO: Use session ID here? will need to specify per-plane if we want to support mesoscope though, which would be experiment ID
        # Although, it would be better if a session was really a session, and had n Planes attached (each currently a different 'experiment')
        nwb_path = self.get_cache_path(None, self.SESSION_NWB_KEY, experiment_id)
        trial_response_path = self.get_cache_path(None, self.TRIAL_RESPONSE_KEY, experiment_id)
        flash_response_path = self.get_cache_path(None, self.FLASH_RESPONSE_KEY, experiment_id)
        extended_stim_path = self.get_cache_path(None, self.EXTENDED_STIM_COLUMNS_KEY, experiment_id)

        #  fetch_api_fns = zip(
        #      [self.fetch_api.get_session_nwb,
        #       self.fetch_api.get_session_trial_response,
        #       self.fetch_api.get_session_flash_response,
        #       self.fetch_api.get_session_extended_stimulus_columns],
        #      [nwb_path, 
        #       trial_response_path,
        #       flash_response_path,
        #       extended_stim_path]
        #  )
        fetch_api_fns = zip(
            [self.fetch_api.get_session_nwb],
            [nwb_path]
        )

        for fetch_api_fn, path in fetch_api_fns:
            call_caching(fetch_api_fn, 
                         path, 
                         experiment_id=experiment_id, 
                         strategy='lazy',
                         writer=write_from_stream)

        session_api = BehaviorOphysNwbApi(
            path=nwb_path,
        )
        return BehaviorOphysSession(api=session_api)

    def add_manifest_paths(self, manifest_builder):
        manifest_builder = super(BehaviorProjectCache, self).add_manifest_paths(manifest_builder)
                                  
        manifest_builder.add_path(
            self.SESSIONS_KEY, 'sessions.csv', parent_key='BASEDIR', typename='file'
        )

        manifest_builder.add_path(
            self.NWB_DIR_KEY, 'nwb_files', parent_key='BASEDIR', typename='dir'
        )

        manifest_builder.add_path(
            self.ANALYSIS_FILES_DIR_KEY, 'analysis_files', parent_key='BASEDIR', typename='dir'
        )

        manifest_builder.add_path(
            self.SESSION_NWB_KEY, 'behavior_ophys_session_%d.nwb', parent_key=self.NWB_DIR_KEY, typename='file'
        )

        manifest_builder.add_path(
            self.TRIAL_RESPONSE_KEY, 'trial_response_df_%d.h5', parent_key=self.ANALYSIS_FILES_DIR_KEY, typename='file'
        )
        
        manifest_builder.add_path(
            self.FLASH_RESPONSE_KEY, 'flash_response_df_%d.h5', parent_key=self.ANALYSIS_FILES_DIR_KEY, typename='file'
        )

        manifest_builder.add_path(
            self.EXTENDED_STIM_COLUMNS_KEY, 'extended_stimulus_presentations_df_%d.h5', parent_key=self.ANALYSIS_FILES_DIR_KEY, typename='file'
        )

        manifest_builder.add_path(
            self.EXPOSURE_NUMBER_KEY, 'exposure_number_%d.h5', parent_key=self.ANALYSIS_FILES_DIR_KEY, typename='file'
        )


        return manifest_builder

    @classmethod
    def from_lims(cls, lims_kwargs=None, **kwargs):
        lims_kwargs = {} if lims_kwargs is None else lims_kwargs
        return cls(
            fetch_api=BehaviorProjectLimsApi.default(**lims_kwargs), 
            **kwargs
        )

    #  @classmethod
    #  def from_warehouse(cls, warehouse_kwargs=None, **kwargs):
    #      warehouse_kwargs = {} if warehouse_kwargs is None else warehouse_kwargs
    #      return cls(
    #          fetch_api=EcephysProjectWarehouseApi.default(**warehouse_kwargs), 
    #          **kwargs
    #      )

    @classmethod
    def fixed(cls, **kwargs):
        return cls(fetch_api=BehaviorProjectFixedApi(), **kwargs)

class InternalCacheFromLims(BehaviorProjectCache):

    def __init__(self):
        fetch_api = BehaviorProjectLimsApi.default()
        self.cache=False
        super().__init__(fetch_api)
        self.SCIENTIFICA_RIG_NAMES = ['CAM2P.3', 'CAM2P.4', 'CAM2P.5']
        self.MESOSCOPE_RIG_NAMES = ['MESO.1']

    def get_sessions(self, **get_sessions_kwargs):
        '''
        Bypass call_caching so we can be flexible with kwargs to this call for now. 
        '''
        return self.fetch_api.get_sessions(**get_sessions_kwargs)
    
    def get_session(self, ophys_session_id):
        # Return regular session from lims if scientifica, otherwise return mesoscope session
        experiment_table = self.get_sessions() #Actually returns experiments
        experiments_this_session_id = experiment_table.query("ophys_session_id == @ophys_session_id")

        if len(experiments_this_session_id)>0:
            equipment_name = experiments_this_session_id['equipment_name'].unique()
            assert len(equipment_name)==1
            equipment_name = equipment_name[0]

            if equipment_name in self.SCIENTIFICA_RIG_NAMES:
                assert len(experiments_this_session_id)==1
                ophys_experiment_id = experiments_this_session_id.iloc[0]['ophys_experiment_id']
                api = BehaviorOphysLimsApi(ophys_experiment_id)
                session = BehaviorOphysSession(api)

            elif equipment_name in self.MESOSCOPE_RIG_NAMES:
                #  assert len(experiments_this_session_id)==8
                session = meso.MesoscopeSession.from_lims(ophys_session_id)

            else:
                raise ValueError("Unknown equipment name: {}".format(equipment_name))

        else:
            raise ValueError("Session ID not in the cache")

        return session

    def get_behavior_only_session(self, behavior_session_id):
        api = PickleFileApi(behavior_session_id)
        session = BehaviorOphysSession(api)
        return session

    def get_all_behavior_sessions(self,
                                  donor_id,
                                  exclude_imaging_sessions = False,
                                  imaging_rigs = ['CAM2P.3', 'CAM2P.4', 'CAM2P.5']):
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
        WHERE d.id = {}
        '''.format(donor_id)

        all_sessions = pd.read_sql(all_behavior_sessions_query, postgres_api.get_connection())
        if exclude_imaging_sessions:
            all_sessions = all_sessions.query('equipment_name not in @imaging_rigs')
        all_sessions = all_sessions.sort_values('created_at').reset_index(drop=True)
        return all_sessions

if __name__=="__main__":
    import time
    t1 = time.time()
    cache = InternalCacheFromLims()
    t2 = time.time()
    print("Initialized cache in {} sec".format(t2-t1))
    sessions = cache.get_sessions()
    t3 = time.time()
    print("Executed manifest query in {} sec".format(t3-t2))
    osid = sessions.iloc[0]['ophys_session_id']
    session = cache.get_session(osid)
    t4 = time.time()
    print("Loaded session in {} sec".format(t4-t3))
    d = sessions.iloc[0]['donor_id']
    bsessions = cache.get_all_behavior_sessions(d, exclude_imaging_sessions=True)
    t5 = time.time()
    print("Executed behavior session query in {} sec".format(t5-t4))
    bsid = bsessions.iloc[0]['behavior_session_id']
    bsession = cache.get_behavior_only_session(bsid)
    t6 = time.time()
    print("Loaded behavior only session in {} sec".format(t6-t5))
    t7 = time.time()
    trial_response = session.trial_response_df
    t8 = time.time()
    print("Calculated trial response df in {} sec".format(t8-t7))

    t9 = time.time()
    stimulus_response = session.stimulus_response_df
    t10 = time.time()
    print("Calculated trial response df in {} sec".format(t10-t9))
