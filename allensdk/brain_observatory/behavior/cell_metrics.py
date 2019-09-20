## For calculating cell level response metrics
import numpy as np
import pandas as pd


def get_mean_response(cell_responses):
    '''
        Get the trial averaged response for some condition
        Args:
            (array or list) cell_responses: array or list of cell responses for a set of stimulus presentations
        Returns
            (float) mean across all provided cell_responses
    '''
    return np.mean(cell_responses)

def get_fraction_significant_trials(cell_p_values):
    '''
       Get the fraction of trials where the p_value is < 0.05 for a cell
       Args:
           (array or list) cell_p_values: array or list of p_values for a set of stimulus presentations for one cell
       Returns
           (float) fraction of stimulus_presentations where p_value < 0.05
    '''
    return len(cell_p_values[cell_p_values<0.05])/float(len(cell_p_values))

