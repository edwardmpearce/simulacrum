#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file can be imported as a module and contains the following functions:
    * view_by_field - View the subsection of the total dataframe matching a given data field
    * plot_by_category - Produce plots of categories within a data field and their test-statistic performance
	* get_subtable - A helper function for plots_by_category to format the table to appear along the x-axis of the plot
"""


import numpy as np

from params import plot_params_dict


__author__ = 'Edward Pearce'
__copyright__ = 'Copyright 2019, Simulacrum Test Suite'
__credits__ = ['Edward Pearce']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'Edward Pearce'
__email__ = 'edward.pearce@phe.gov.uk'
__status__ = 'Development'


def view_by_field(df, col_name, sort_by=['val']):
    r"""View the slice of the dataframe where the col_name value matches the passed column name, sorted by the category names"""
    return df.loc[(df.col_name == col_name)].sort_values(by=sort_by)


def get_subtable(frame, plot_params):
    r"""A helper function for plots_by_category to format the table to appear along the x-axis of the plot"""
    if plot_params['y_label'] == 'Counts':
        return True
    else:
        return np.round(frame[['val'] + plot_params['y_values']].set_index('val').T, 3)


def plot_by_category(df, col_name, plot_type='Proportions'):
    r"""Produces an appropriate plot to compare value counts of a column in two tables.

    For example, grouped bar chart to compare value counts, proportions of a category occurring in a table.
    Two line plots for semi-continuous data such as age or dates.

    Parameters
    ----------
    df : pandas DataFrame object
        The dataframe from which we draw the data to plot
    col_name : str
        The name of the column in our dataframe we use to obtain the values for the x-axis.
    plot_type : str
        The type of data we want to plot along the y-axis.

    Returns
    -------
    matplotlib Axes object
        The Axes object containing our plot

    """
    if col_name in ['DATE_FIRST_SURGERY', 'DIAGNOSISDATEBEST']:
        return
    # Setup the plot parameters
    frame = view_by_field(df, col_name)
    plot_params = plot_params_dict[plot_type]

    if plot_type == 'One-sample Binomial z-test':
        colour_scheme = frame.apply(lambda x: 'orange' if (x['one_sample_z_approx_valid'] == 0) else ('blue' if (abs(x['binom_z_test_one_sample']) < 4) else 'red'), axis=1)
        ax = frame.plot.bar('val', plot_params['y_values'], color=[colour_scheme], label=plot_params['labels'], figsize=(15, 6), fontsize=18,
                   table=get_subtable(frame, plot_params))
    elif plot_type == 'Pooled Two-sample Binomial z-test':
        colour_scheme = frame.apply(lambda x: 'blue' if (abs(x['z_test_two_sample_pooled']) < 4) else 'red', axis=1)
        ax = frame.plot.bar('val', plot_params['y_values'], color=[colour_scheme], label=plot_params['labels'], figsize=(15, 6), fontsize=18,
                   table=get_subtable(frame, plot_params))
    else:
        # Make the plot
        ax = frame.plot.bar('val', plot_params['y_values'], label=plot_params['labels'], figsize=(15, 6), fontsize=18,
                   table=get_subtable(frame, plot_params))
    # Set the axes labels and title
    ax.get_xaxis().set_visible(False)
    ax.set_xlabel(plot_params['x_label'], fontsize = 24) 
    ax.set_ylabel(plot_params['y_label'], fontsize = 24) 
    ax.set_title(plot_params['title'].format(col_name), fontsize = 24) 
    return ax
