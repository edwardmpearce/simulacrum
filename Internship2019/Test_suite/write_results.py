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
    if row[column_id] == 'AGE':
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


def categorical_separator(chunked_dfs):
    r"""Separate counts grouped by either 'DIAGNOSISDATEBEST' or 'DATE_FIRST_SURGERY' into a frame separate from the rest"""
    date_rows = []
    categorical_rows = []
    length_check = 0
    for frame in chunked_dfs:
        length_check += frame.shape[0]
        date_rows.append(frame.loc[frame[['column_name1', 'column_name2']].isin(['DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY']).any(axis=1)])
        categorical_rows.append(frame.loc[~frame[['column_name1', 'column_name2']].isin(['DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY']).any(axis=1)])
    
    date_frame = pd.concat(date_rows)
    categorical_frame = pd.concat(categorical_rows)
    print('Date rows ({}) + Categorical rows ({}) = total rows ({})? {}'.format(
        date_frame.shape[0], categorical_frame.shape[0], length_check, date_frame.shape[0]+categorical_frame.shape[0]==length_check))
    return date_frame, categorical_frame


def dates_separator(chunked_dfs):
    r"""Separate counts grouped by either 'DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY', or both, into 3 frames"""
    double_date_rows = []
    diagnosis_date_rows = []
    surgery_date_rows = []
    length_check = 0
    for frame in chunked_dfs:
        length_check += frame.shape[0]
        double_date_rows.append(frame.loc[frame[['column_name1', 'column_name2']].isin(['DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY']).all(axis=1)])
        diagnosis_date_rows.append(frame.loc[frame[['column_name1', 'column_name2']].isin(['DIAGNOSISDATEBEST']).any(axis=1) & 
                                             (~frame[['column_name1', 'column_name2']].isin(['DATE_FIRST_SURGERY']).any(axis=1))])
        surgery_date_rows.append(frame.loc[frame[['column_name1', 'column_name2']].isin(['DATE_FIRST_SURGERY']).any(axis=1) & 
                                             (~frame[['column_name1', 'column_name2']].isin(['DIAGNOSISDATEBEST']).any(axis=1))])
    double_date_frame = pd.concat(double_date_rows)
    diagnosis_date_frame = pd.concat(diagnosis_date_rows)
    surgery_date_frame = pd.concat(surgery_date_rows)
    print('{} double date rows + {} diagnosis date rows + {} surgery date rows = {} total rows? {}'.format(
        double_date_frame.shape[0], diagnosis_date_frame.shape[0], surgery_date_frame.shape[0], length_check, 
        sum([double_date_frame.shape[0], diagnosis_date_frame.shape[0], surgery_date_frame.shape[0]])==length_check))
    return double_date_frame, diagnosis_date_frame, surgery_date_frame


def reorder_columns(table, to_front='DIAGNOSISDATEBEST', suffix=''):
    r"""Moves rows with column_name value equal to the passed `to_front` argument into the `column_name1` column."""
    no_swap = table.loc[table.column_name1 == to_front]
    to_swap = table.loc[table.column_name2 == to_front]
    # Change column names
    to_swap.columns = ['column_name2', 'column_name1', 'val2_clean', 'val1_clean', 'paired_counts_' + suffix]
    reordered = pd.concat([to_swap, no_swap], sort=True)
    print('Shape check succussful? {} = {}? {}'.format(reordered.shape, table.shape, reordered.shape == table.shape))
    print(to_front, 'moved to column_name1?', (reordered.column_name1 == to_front).all())
    sorted_output = pd.concat([reordered.loc[reordered.column_name2 == col_name].sort_values(by=['val2_clean', 'val1_clean']) for col_name in col_names])
    sorted_output = sorted_output[['column_name1', 'column_name2', 'val1_clean', 'val2_clean', 'paired_counts_' + suffix]]
    return sorted_output


def write_univariate_categorical_counts_to_csv():
    for key, pop_query in pop_queries.items():
        frame =  pd.read_sql_query(queries.make_totals_query(pop_query, key, field_list=categorical_cols), db)
        frame['column_name'] = frame['column_name'].astype('category')
        frame['counts_'+key] = frame['counts_'+key].astype('uint32')
        frame['val'] = frame.apply(lambda row: int(row['val']) if row['column_name'] == 'AGE' else row['val'], axis=1)
        # Sort the dataframe by column name, then by value
        frame = pd.concat([frame.loc[frame.column_name == col_name].sort_values(by='val') for col_name in categorical_cols])
        frame.to_csv(r"results\{}_univariate_categorical.csv".format(key.upper()), index=False)


def write_univariate_date_counts_to_csv():
    for key, pop_query in pop_queries.items():
        frame =  pd.read_sql_query(queries.make_totals_query(pop_query, key, field_list=date_cols), db)
        frame['column_name'] = frame['column_name'].astype('category')
        frame['counts_'+key] = frame['counts_'+key].astype('uint32')
        frame['val'] = pd.to_datetime(frame['val'], infer_datetime_format=True, errors='coerce')
        # Sort the dataframe by column name, then by value
        frame = pd.concat([frame.loc[frame.column_name == col_name].sort_values(by='val') for col_name in date_cols])
        frame.to_csv(r"results\{}_univariate_dates.csv".format(key.upper()), index=False)


def write_bivariate_categorical_counts_to_csv():
    for key, pop_query in pop_queries.items():
        frame =  pd.read_sql_query(queries.make_totals_query(pop_query, key, 
                                                             field_list=categorical_col_pairs, num_variates=2), db)
        frame[['column_name1', 'column_name2']] = frame[['column_name1', 'column_name2']].astype('category')
        frame['counts_'+key] = frame['counts_'+key].astype('uint32')
        # By design, 'AGE' should always be in column_name2 when it appears in a pair
        frame['val1'] = frame.apply(lambda row: int(row['val1']) if row['column_name1'] == 'AGE' else row['val1'], axis=1)
        frame['val2'] = frame.apply(lambda row: int(row['val2']) if row['column_name2'] == 'AGE' else row['val2'], axis=1)
        # Sort the dataframe by column name, then by value
        frame = pd.concat([frame.loc[
            (frame.column_name1 == pair[0]) & (frame.column_name2 == pair[1])]
                                   .sort_values(by=['val1', 'val2']) for pair in categorical_col_pairs])
        frame.to_csv(r"results\{}_bivariate_categorical.csv".format(key.upper()), index=False)


def write_bivariate_date_counts_to_csv():
    for key, pop_query in pop_queries.items():
        frame =  pd.read_sql_query(queries.make_totals_query(pop_query, key, 
                                                             field_list=category_cross_date_pairs, num_variates=2), db)
        frame[['column_name1', 'column_name2']] = frame[['column_name1', 'column_name2']].astype('category')
        frame['counts_'+key] = frame['counts_'+key].astype('uint32')
        # By design, date fields should always be in column_name2 when appearing in the pairs
        frame['val1'] = frame.apply(lambda row: int(row['val1']) if row['column_name1'] == 'AGE' else row['val1'], axis=1)
        frame['val2'] = pd.to_datetime(frame['val2'], infer_datetime_format=True, errors='coerce')
        # Sort the dataframe by column name, then by value
        frame = pd.concat([frame.loc[
            (frame.column_name1 == pair[0]) & (frame.column_name2 == pair[1])]
                                   .sort_values(by=['val1', 'val2']) for pair in category_cross_date_pairs])
        diagnosis_date_frame = frame.loc[frame.column_name2 == 'DIAGNOSISDATEBEST']
        surgery_date_frame = frame.loc[frame.column_name2 == 'DATE_FIRST_SURGERY']
        diagnosis_date_frame.to_csv(r"results\{}_categorical_cross_diagnosis_dates.csv".format(key.upper()), index=False)
        surgerys_date_frame.to_csv(r"results\{}_categorical_cross_surgery_dates.csv".format(key.upper()), index=False)
