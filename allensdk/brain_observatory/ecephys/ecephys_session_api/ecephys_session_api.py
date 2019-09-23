from typing import Dict
from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr

from ...running_speed import RunningSpeed


class EcephysSessionApi:

    session_na = -1

    __slots__: tuple = tuple([])

    def __init__(self, *args, **kwargs):
        pass

    def get_session_start_time(self) -> datetime:
        raise NotImplementedError

    def get_running_speed(self) -> RunningSpeed:
        raise NotImplementedError

    def get_stimulus_presentations(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_invalid_times(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_probes(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_channels(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_mean_waveforms(self) -> Dict[int, np.ndarray]:
        raise NotImplementedError

    def get_spike_times(self) -> Dict[int, np.ndarray]:
        raise NotImplementedError

    def get_units(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_ecephys_session_id(self) -> int:
        raise NotImplementedError

    def get_lfp(self, probe_id: int) -> xr.DataArray:
        raise NotImplementedError

    def get_optogenetic_stimulation(self) -> pd.DataFrame:
        raise NotImplementedError

    def get_spike_amplitudes(self) -> Dict[int, np.ndarray]:
        raise NotImplementedError

    def get_eye_tracking_ellipse_fit_data(self):
        raise NotImplementedError

    def get_raw_eye_gaze_mapping_data(self):
        raise NotImplementedError

    def get_filtered_eye_gaze_mapping_data(self):
        raise NotImplementedError

    def get_metadata(self):
        raise NotImplementedError