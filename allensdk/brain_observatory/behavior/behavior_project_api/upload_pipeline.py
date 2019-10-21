import sys
import pandas as pd
from allensdk.brain_observatory.behavior import behavior_project_cache as bpc
from allensdk.brain_observatory.behavior.behavior_project_api import behavior_ophys_analysis_query_utils as qu

ind_session = int(sys.argv[1])

manifest_path = "/allen/programs/braintv/workgroups/nc-ophys/visual_behavior/SFN_2019/cache_with_extra_inhibitory/behavior_project_manifest.json"
cache = bpc.InternalCacheFromLims(manifest=manifest_path)
sessions = cache.get_sessions()

# It is important that this dataframe of sessions is the same between this upload script and the runner. We should fix this to be independent.
scientifica_sessions = sessions[(sessions['equipment_name']!='MESO.1')&(~pd.isnull(sessions['stage_name']))]
session_entry = scientifica_sessions.iloc[ind_session]
print(session_entry)

print(session_entry['ophys_session_id'])

def write_to_mongo(session_entry):
   #  qu.write_to_manifest_collection(dict(session_entry))
   session = cache.get_session(session_entry['ophys_session_id'])
   qu.write_eventlocked_traces_to_collection(session, force_write=True)
   #  qu.write_stimulus_response_to_collection(session, force_write=True)
   #  qu.write_stimulus_presentations_to_collection(session, force_write=True)

write_to_mongo(session_entry)
