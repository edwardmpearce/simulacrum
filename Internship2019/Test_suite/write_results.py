#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file can be imported as a module and contains the following functions:
    * clean_values - Cleans category names for easier comparison between tables, plotting. 
    * write_counts_to_csv - Writes value counts by field from an SQL table to a .csv file.
"""


import pandas as pd

import queries
from params import col_names, col_name_pairs


__author__ = 'Edward Pearce'
__copyright__ = 'Copyright 2019, Simulacrum Test Suite'
__credits__ = ['Edward Pearce']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'Edward Pearce'
__email__ = 'edward.pearce@phe.gov.uk'
__status__ = 'Development'


def clean_values(row, suffix=''):
    r"""Cleans category names for easier comparison between tables, plotting."""
    column_id = 'column_name' + suffix
    value_id = 'val' + suffix
    if row[column_id] == 'CREG_CODE':
        # Strip the first character of Cancer Registry Code
        return row[value_id][1:]
    elif row[column_id] == 'QUINTILE_2015':
        # Strip extra text, retaining only Deprivation Index score (integer between 1 and 5 inclusive)
        return int(row[value_id][0])
    elif row[column_id] == 'AGE':
        # Cast Age to integer for sorting, plotting
        return int(row[value_id])
    elif row[column_id] in ['DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY']:
        # Cast date fields to datetime objects for sorting, plotting
        return pd.to_datetime(row[value_id], infer_datetime_format=True, errors='coerce')
    else:
        return row[value_id]


def write_counts_to_csv(pop_query, db, suffix, filepath, num_variates=1):
    r"""Writes value counts by field from an SQL table (defined by query) to a .csv file.
    
    Obtains value counts by field from an SQL table, cleans and sorts value/category names, and writes the results to a .csv file.
    
    Parameters
    ----------
    pop_query : str
        The SQL Select statement to obtain the table for which we want to find aggregate information
    db : An instance of an `sqlalchemy.engine`
        This is the connection to your database management system.
    suffix : str
        A suffix added to the 'counts' column name in the final output
    filepath : str
        The location where the .csv file will be written
    
    Returns
    -------
    Boolean
        Returns True if the function was executed successfully
    
    """
    if num_variates not in [1, 2]:
        print('The keyword `num_variates` currently only accepts values in [1,2]')
        return False
    
    print('Getting the data - calculating counts in SQL...')
    raw_output = pd.read_sql_query(queries.make_totals_query(pop_query, suffix, num_variates), db)
    print('Totals pulled from database successfully! ({} rows, {} columns)'.format(raw_output.shape[0], raw_output.shape[1]))
    
    print('Cleaning category names and sorting...')
    if num_variates == 1:  
        raw_output['val_clean'] = raw_output.apply(clean_values, axis=1)
        sorted_output = pd.concat([raw_output.loc[raw_output.column_name == col_name].sort_values(by='val_clean') for col_name in col_names])
        
    elif num_variates == 2:
        for i in ['1', '2']:
            raw_output['val'+ i + '_clean'] = raw_output.apply(lambda row: clean_values(row, suffix=i), axis=1)
        sorted_output = pd.concat([raw_output.loc[
            (raw_output.column_name1 == pair[0]) & (raw_output.column_name2 == pair[1])]
                                   .sort_values(by=['val1_clean', 'val2_clean']) for pair in col_name_pairs])
    print('Data cleaned and sorted!' + '\n' + 'Saving the results at {}'.format(filepath))
    sorted_output.to_csv(filepath, index=False)
    print('Saved! Function complete!')
    return True
