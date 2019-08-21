#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file can be imported as a module and contains the following functions:
    * clean_values - Cleans category names for easier comparison between tables, plotting. 
    * write_counts_to_csv - Writes value counts by field from an SQL table to a .csv file.
"""


import pandas as pd

import queries
from params import col_names


__author__ = 'Edward Pearce'
__copyright__ = 'Copyright 2019, Simulacrum Test Suite'
__credits__ = ['Edward Pearce']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'Edward Pearce'
__email__ = 'edward.pearce@phe.gov.uk'
__status__ = 'Development'


def clean_values(row):
    r"""Cleans category names for easier comparison between tables, plotting."""
    if row.column_name == 'CREG_CODE':
        # Strip the first character of Cancer Registry Code
        return row.val[1:]
    elif row.column_name == 'QUINTILE_2015':
        # Strip extra text, retaining only Deprivation Index score (integer between 1 and 5 inclusive)
        return int(row.val[0])
    elif row.column_name == 'AGE':
        # Cast Age to integer for sorting, plotting
        return int(row.val)
    elif row.column_name in ['DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY']:
        # Cast date fields to datetime objects for sorting, plotting
        return pd.to_datetime(row.val, infer_datetime_format=True, errors='coerce')
    else:
        return row.val


def write_counts_to_csv(pop_query, db, suffix, filepath):
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
    print('Getting the data - calculating counts in SQL...')
    raw_output = pd.read_sql_query(queries.make_totals_query(pop_query, suffix), db)
    print('Totals pulled from database successfully! ({} rows)'.format(raw_output.shape[0]))
    print('Cleaning category names and sorting...')
    raw_output['val_clean'] = raw_output.apply(clean_values, axis=1)
    sorted_output = pd.concat([raw_output.loc[raw_output.column_name == col_name].sort_values(by='val_clean') for col_name in col_names])
    print('Data cleaned and sorted!' + '\n' + 'Saving the results at {}'.format(filepath))
    sorted_output.to_csv(filepath, index=False)
    print('Saved! Function complete!')
    return True
