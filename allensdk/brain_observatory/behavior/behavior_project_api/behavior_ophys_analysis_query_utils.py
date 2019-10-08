from pymongo import MongoClient
import yaml
import pandas as pd
import numpy as np
import json
import os
import glob
import traceback
import datetime

from allensdk.brain_observatory.behavior import behavior_project_cache as bpc
import allensdk.brain_observatory.behavior.cell_metrics as cm


class Database(object):
    '''
    utilities for connecting to MongoDB databases (mouseseeks or visual_behavior_data)

    parameter:
      database: defines database to connect to. Can be 'visual_behavior_data'
    '''

    def __init__(self, database_server='visual_behavior_data', db_info_path='//allen/programs/braintv/workgroups/nc-ophys/visual_behavior', db_info_filename='mongo_db_info.yml'):
        # get database ip/port info from a text file on the network (maybe not a good idea to commit it)

        db_info_filepath = os.path.join(db_info_path, db_info_filename)
        with open(db_info_filepath, 'r') as stream:
            db_info = yaml.safe_load(stream)

        # connect to the client
        ip = db_info[database_server]['ip']
        port = db_info[database_server]['port']
        self.client = MongoClient('mongodb://{}:{}'.format(ip, port))

        # set each database as an attribute of the class (but not admin) and as an entry in a dictionary
        # this will provide flexibility in how the databases are called
        self.database = {}
        self.database_names = []
        databases = [db for db in self.client.list_database_names()
                     if db != 'admin']
        for db in databases:
            self.database_names.append(db)
            self.database[db] = self.client[db]
            setattr(self, db, self.client[db])
        # make subscriptable
        self._db_names = {db: self.client[db] for db in databases}

    def __getitem__(self, item):
        # this allows databases to be accessed by name
        return self._db_names[item]

    def query(self, database, collection, query={}, return_as='dataframe'):
        '''
        Run a query on a collection in the database.
        The query should be formated as set of key/value pairs
        Sending an empty query will return the entire collection
        '''

        return pd.DataFrame(list(self.database[database][collection].find(query)))

    def close(self):
        '''
        close connection to client
        '''
        self.client.close()


def is_int(n):
    return isinstance(n, (int, np.integer))

def is_float(n):
    return isinstance(n, (float, np.float))

def simplify_type(x):
    if is_int(x):
        return int(x)
    elif is_float(x):
        return float(x)
    else:
        return x

def simplify_entry(entry):
    '''
    entry is one document
    '''
    entry = {k: simplify_type(v) for k, v in entry.items()}
    return entry

def clean_and_timestamp(entry):
    '''make sure float and int types are basic python types (e.g., not np.float)'''
    entry = simplify_entry(entry)
    entry.update({'entry_time_utc': str(datetime.datetime.utcnow())})
    return entry


def update_or_create(collection, document, keys_to_check):
    '''
    updates a collection of the document exists
    inserts if it does not exist
    uses keys in `keys_to_check` to determine if document exists. Other keys will be written, but not used for checking uniqueness
    '''
    query = {key:simplify_type(document[key]) for key in keys_to_check}
    if collection.find_one(query) is None:
        # insert a document if this experiment/cell doesn't already exist
        collection.insert_one(simplify_entry(document))
    else:
        # update a document if it does exist
        collection.update_one(query, {"$set": simplify_entry(document)})

def write_to_manifest_collection(manifest_entry, overwrite=False, server='visual_behavior_data'):
    '''
    * single table
    * each row will be a document
    * index by: 
     * container_id
     * ophys_experiment_id
     * ophys_session_id
     * foraging_id
    * make a convenience function to retrieve the entire manifest
    '''
    vb = Database(server)

    entry = clean_and_timestamp(dict(manifest_entry))

    update_or_create(
        vb['ophys_data']['manifest'],
        entry,
        keys_to_check = ['ophys_session_id']
    )
    vb.close()


def get_manifest(server='visual_behavior_data'):
    '''
    convenience function to get full manifest
    '''
    vb = Database(server)
    man = vb['ophys_data']['manifest'].find({})
    vb.close()
    return pd.DataFrame(list(man))


def write_to_dff_traces_collection(session, overwrite=False, server='visual_behavior_data'):
    '''
    * each row will be a document
    * index by:
     * cell_specimen_id
     * cell_roi_id
     * ophys_experiment_id
     * ophys_session_id (not implemented)
     * foraging_id (not implemented)
    '''

    dff_traces = session.dff_traces.reset_index()
    dff_traces['ophys_experiment_id'] = session.ophys_experiment_id

    vb = Database(server)
    for idx, row in dff_traces.iterrows():
        entry = row.to_dict()
        res = vb['ophys_data']['dff_traces'].find_one(
            {'cell_roi_id': entry['cell_roi_id']})
        if res is None:
            # cast to simple int or float
            entry = clean_and_timestamp(entry)
            entry['dff'] = entry['dff'].tolist()  # cast array to list

            # maybe come back to this...
            update_or_create(
                vb['ophys_data']['dff_traces'],
                entry,
                keys_to_check = ['ophys_experiment_id','cell_specimen_id','cell_roi_id']
            )
        else:
            pass
#             print('record for cell_roi_id {} already exists'.format(entry['cell_roi_id']))
    vb.close()


def get_dff_traces(query={}, server='visual_behavior_data'):
    '''
    returns dff_traces table
    pass query in the form:
        {KEY:value}
    an empty query will return the entire table
    '''
    vb = Database(server)
    df = pd.DataFrame(list(vb['ophys_data']['dff_traces'].find(query)))
    vb.close()
    return df


def write_stimulus_response_to_collection(session, server='visual_behavior_data'):
    '''
    * each row will be a document
    * save before merging in stimulus_presentations
    * index by:  
     * stimulus_presentations_id
     * cell_specimen_id
     * ophys_experiment_id
     * ophys_session_id (not implemented)
     * foraging_id (not implemented)

     NOTE: dropping dff_trace and dff_trace_timestamps
           both are saved elsewhere as xarray, can be merged back in later
     '''

    vb = Database(server)

    res = vb['ophys_data']['stimulus_response'].find_one(
        {'ophys_experiment_id': int(session.ophys_experiment_id)})
    if res is None:

        df = session.stimulus_response_df.drop(columns=['dff_trace', 'dff_trace_timestamps']).merge(session.stimulus_presentations['image_name'], 
                                                                                                    left_on='stimulus_presentations_id', 
                                                                                                    right_index=True)
        for idx, row in df.reset_index().iterrows():
            entry = {'ophys_experiment_id': int(session.ophys_experiment_id)}
            entry.update(row.to_dict())
            entry = clean_and_timestamp(entry)
            update_or_create(
                vb['ophys_data']['stimulus_response'],
                entry,
                keys_to_check = ['ophys_experiment_id','cell_specimen_id','stimulus_presentations_id']
            )
    else:
        pass
#         print('experiment {} already in table'.format(session.ophys_experiment_id))
    vb.close()

def write_eventlocked_traces_to_collection(session, server='visual_behavior_data'):

    vb = Database(server)

    oeid = int(session.ophys_experiment_id)

    stim_response_xr_stacked = session.stimulus_response_xr['eventlocked_traces'].stack(multi_index=('cell_specimen_id','stimulus_presentations_id'))
    print("uploading stimulus response traces for oeid: {}\n".format(oeid))
    for trace_ind in list(range(stim_response_xr_stacked.shape[1])):
        trace_xr = stim_response_xr_stacked[:, trace_ind]
        document = {
            'ophys_experiment_id': int(oeid),
            'cell_specimen_id': int(trace_xr.coords['multi_index'].data.item()[0]),
            'stimulus_presentations_id': int(trace_xr.coords['multi_index'].data.item()[1]),
            't_0': float(trace_xr.coords['eventlocked_timestamps'][0].data.item()),
            't_f': float(trace_xr.coords['eventlocked_timestamps'][-1].data.item()),
            'dff': trace_xr.data.astype(float).tolist()
        }
        document = clean_and_timestamp(document)
        update_or_create(
            vb['ophys_data']['stimulus_response_traces'],
            document,
            keys_to_check = ['ophys_experiment_id','cell_specimen_id','stimulus_presentations_id']
        )

    vb.close()


def get_stimulus_response(query=None, server='visual_behavior_data'):
    '''
    returns stimulus_response table
    pass query in the form:
        {KEY:value}
    can query on:
      * ophys_experiment_id
    an empty query will return the entire table (all experiments)

    Note that 'dff_traces' and 'dff_trace_timestamps' have been dropped from the db
    TO IMPLEMENT: get 'dff_traces' and 'dff_trace_timestamps' from a cached xarray and add them back in to the result

    '''
    if query == None:
        query={}
    vb = Database(server)
    res = list(vb['ophys_data']['stimulus_response'].find(query))
    vb.close()

    cols = ['stimulus_presentations_id', 'cell_specimen_id',
            'mean_response', 'baseline_response', 'p_value']
    dfs = []
    for s in res:
        df = pd.DataFrame()
        for col in cols:
            df[col] = s[col]
        dfs.append(df)
    df = pd.concat(dfs)

    return df


def write_stimulus_presentations_to_collection(session, server='visual_behavior_data'):
    '''
    * index by:  
     * ophys_experiment_id
     * ophys_session_id (not implemented)
     * foraging_id (not implemented)
    * each session will be one document

     NOTE: dropping rewards and licks. Should those be reimplimented later"
     '''
    vb = Database(server)

    res = vb['ophys_data']['stimulus_presentations'].find_one(
        {'ophys_experiment_id': int(session.ophys_experiment_id)})

    #  if res is None:
    if True:
        df = session.extended_stimulus_presentations.drop(
            ['licks', 'rewards'], axis=1).reset_index()

        entry = {'ophys_experiment_id': int(session.ophys_experiment_id)}
        for col in df.columns:
            entry.update({col: df[col].values.tolist()})
        entry = clean_and_timestamp(entry)

        update_or_create(
            vb['ophys_data']['stimulus_presentations'],
            entry,
            keys_to_check = ['ophys_experiment_id']
        )

    else:
        pass
#         print('experiment {} already in table'.format(session.ophys_experiment_id))
    vb.close()


def get_stimulus_presentations(query=None, server='visual_behavior_data'):
    '''
    returns stimulus_response table
    pass query in the form:
        {KEY:value}
    can query on:
      * ophys_experiment_id
    an empty query will return the entire table (all experiments)

    Note that 'rewards' and 'licks' have been dropped from the db

    '''
    if query == None:
        query = {}
    vb = Database(server)
    res = list(vb['ophys_data']['stimulus_presentations'].find(query))
    vb.close()

    cols = ['stimulus_presentations_id', 'index', 'block_index', 'index_within_block', 'change', 'duration',
            'start_time', 'start_frame', 'end_frame',
            'stop_time', 'image_index', 'image_name', 'image_set',
            'omitted', 'orientation', 'time_from_last_lick', 'time_from_last_reward',
            'time_from_last_change', 'image_block_repetition', 'mean_running_speed'
            ]

    dfs = []
    for s in res:
        df = pd.DataFrame()
        for col in cols:
            df[col] = s[col]
        dfs.append(df.set_index('stimulus_presentations_id'))
    df = pd.concat(dfs)

    return df


def write_metrics_to_collection(metrics_df, server='visual_behavior_data'):
    '''
    each session/cell combo is a distinct document
    '''
    df = metrics_df.copy()
    vb = Database(server)

    df.rename(columns={'experiment_id': 'ophys_experiment_id'}, inplace=True)
    for idx, row in df.reset_index().iterrows():
        entry = clean_and_timestamp(row.to_dict())

        update_or_create(
            vb['ophys_data']['metrics'],
            entry,
            keys_to_check = ['ophys_experiment_id','cell_specimen_id']
        )

    vb.close()


def get_metrics(query=None, server='visual_behavior_data'):
    vb = Database(server)
    if query is None:
        query = {}
    df = pd.DataFrame(list(vb['ophys_data']['metrics'].find(query)))
    df['ophys_experiment_id'] = df['ophys_experiment_id'].astype(int)
    df['cell_specimen_id'] = df['cell_specimen_id'].astype(int)
    vb.close()
    return df
