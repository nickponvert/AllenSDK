from allensdk.core.lazy_property import LazyProperty, LazyPropertyMixin
# from allensdk.internal.api.mesoscope_lims_api import MesoscopeLimsApi
import pandas as pd
from allensdk.brain_observatory.behavior.behavior_ophys_session import BehaviorOphysSession as MesoscopeOphysPlane
from allensdk.internal.api.mesoscope_lims_api import MesoscopeSessionLimsApi, MesoscopePlaneLimsApi

class MesoscopeSession(LazyPropertyMixin):

    @classmethod
    def from_lims(cls, session_id):
        return cls(api=MesoscopeSessionLimsApi(session_id))

    def __init__(self, api=None):

        self.api = api
        # self.session_id = LazyProperty(self.api.get_session_id)
        # self.metadata = LazyProperty(self.api.get_metadata)
        # self.session_df = LazyProperty(self.api.get_session_df)
        self.experiment_ids = LazyProperty(self.api.get_session_experiments)
        # self.pairs = LazyProperty(self.api.get_paired_experiments)
        # self.splitting_json =LazyProperty(self.api.get_splitting_json)
        # self.folder = LazyProperty(self.api.get_session_folder)

    def get_exp_by_structure(self, structure):
        return self.experiments.loc[self.session_df.structure == structure]

    def get_planes(self):

        for experiment_id in self.experiment_ids:
            api = MesoscopePlaneLimsApi(903485718)
            print(MesoscopeOphysPlane(api=api))



if __name__ == "__main__":

    session = MesoscopeSession.from_lims(754606824)
    # print(session.session_id)
    # pd.options.display.width = 0
    print(session.get_planes())
    # print(session.session_df)
    # print(session.folder)
    # print(session.splitting_json)
    # print(session.pairs)
    # for exp in session.experiments['experiment_id']:
    #     print(session.api.get_experiment_df(exp))

    # print(session.metadata)


    # @classmethod
    # def from_lims(cls, ophys_experiment_id):
    #     return cls(api=BehaviorOphysLimsApi(ophys_experiment_id))


    # from allensdk.internal.api.behavior_ophys_api import BehaviorOphysLimsApi
    


