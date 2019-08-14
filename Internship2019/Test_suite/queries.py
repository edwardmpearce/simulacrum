#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file can be imported as a module and contains the following functions:
    * get_cols_query - returns an SQL query to get the column names of a table in a database
    * make_AV2017_pop_query - returns an SQL query to construct a table of tumour data from the AV2017 snapshot    
    * make_totals_query - returns an SQL query to get counts of values in a list of columns
    * all_counts_query - returns an SQL query to get linked value counts from a list of columns in a pair of datasets.
"""

__author__ = 'Edward Pearce'
__copyright__ = 'Copyright 2019, Simulacrum Test Suite'
__credits__ = ['Edward Pearce', 'Paul Clarke', 'Cong Chen']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'Edward Pearce'
__email__ = 'edward.pearce@phe.gov.uk'
__status__ = 'Development'


def get_cols_query(owner, table, condition=""):
    r"""Returns SQL query to get column names from a given table where additional conditions may be specified"""
    query = '''SELECT column_name FROM all_tab_cols WHERE owner = '{}' AND table_name = '{}' '''.format(owner, table)
    if condition != "":
        query = query + "AND {}".format(condition)
    return query


def make_AV2017_pop_query():
    r"""Returns SQL query to build a table of tumour data drawn from the AV2017 snapshot ready for comparison with
        its counterpart in the Simulacrum."""
    
    at_tumour_cols = 'TUMOURID, LSOA11_CODE, GRADE, AGE, SEX, CREG_CODE, SCREENINGSTATUSFULL_CODE, ER_STATUS, ER_SCORE, PR_STATUS, PR_SCORE, HER2_STATUS, GLEASON_PRIMARY, GLEASON_SECONDARY, GLEASON_TERTIARY, GLEASON_COMBINED, LATERALITY, DIAGNOSISDATEBEST, SITE_ICD10_O2, SITE_ICD10_O2_3CHAR, MORPH_ICD10_O2, BEHAVIOUR_ICD10_O2, T_BEST, N_BEST, M_BEST, STAGE_BEST, STAGE_BEST_SYSTEM'

    at_tumour_exp_cols = 'TUMOURID, CANCERCAREPLANINTENT, PERFORMANCESTATUS, CNS, ACE27, DATE_FIRST_SURGERY'

    all_cols_joined = 'multi_depr_index.QUINTILE_2015, at_tumour.GRADE, at_tumour.AGE, at_tumour.SEX, at_tumour.CREG_CODE, at_tumour.SCREENINGSTATUSFULL_CODE, at_tumour.ER_STATUS, at_tumour.ER_SCORE, at_tumour.PR_STATUS, at_tumour.PR_SCORE, at_tumour.HER2_STATUS, at_tumour.GLEASON_PRIMARY, at_tumour.GLEASON_SECONDARY, at_tumour.GLEASON_TERTIARY, at_tumour.GLEASON_COMBINED, at_tumour.LATERALITY, at_tumour.DIAGNOSISDATEBEST, at_tumour.SITE_ICD10_O2, at_tumour.SITE_ICD10_O2_3CHAR, at_tumour.MORPH_ICD10_O2, at_tumour.BEHAVIOUR_ICD10_O2, at_tumour.T_BEST, at_tumour.N_BEST, at_tumour.M_BEST, at_tumour.STAGE_BEST, at_tumour.STAGE_BEST_SYSTEM, at_tumour_exp.CANCERCAREPLANINTENT, at_tumour_exp.PERFORMANCESTATUS, at_tumour_exp.CNS, at_tumour_exp.ACE27, at_tumour_exp.DATE_FIRST_SURGERY'
    
    AV2017_pop_query = '''SELECT {all_cols_joined} FROM
(SELECT {at_tumour_cols} FROM AV2017.AT_TUMOUR_ENGLAND WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1) at_tumour
LEFT JOIN 
(SELECT {at_tumour_exp_cols} 
FROM AV2017.AT_TUMOUR_EXPERIMENTAL_ENGLAND) at_tumour_exp
ON at_tumour.tumourid = at_tumour_exp.tumourid
LEFT JOIN IMD.ID2015 multi_depr_index
ON at_tumour.LSOA11_CODE = multi_depr_index.LSOA11_CODE
'''.replace('\n', ' ').format(all_cols_joined=all_cols_joined, 
                              at_tumour_cols=at_tumour_cols, 
                              at_tumour_exp_cols=at_tumour_exp_cols)
    
    return AV2017_pop_query


def make_totals_query(pop_query, col_names, suffix='', standalone=True):
    r"""Compose a large SQL query to obtain counts of values in a list of columns.
    
    Concatenates subqueries which obtain value counts for the passed columns within the passed table (defined by query).
    
    Parameters
    ----------
    pop_query : str
        The SQL Select statement to obtain the table for which we want to find aggregate information
    col_names : list
        The list of table columns for which we compute value counts
    suffix : str, optional
        A suffix added to the 'counts' column name in the final output
    standalone : Boolean, defaults to True
        Set to False if the output will be a subquery, in order to avoid nested WITH statements.
    
    Returns
    -------
    str
        An SQL query prepped for input into pd.read_sql_query
    
    """    
    # This is the template for the subquery obtaining group counts, 
    # along with a 'UNION ALL' statement to join the smaller tables together
    sql_grouped_count_template = '''SELECT
'{col_name}' AS column_name,
TO_CHAR({col_name}) AS val,
COUNT(*) AS counts_{suffix}
FROM population_{suffix}
GROUP BY {col_name}
UNION ALL
'''.replace('\n', ' ').replace('{suffix}', suffix)

    # Here we initiliaze our long string of SQL code
    sql_get_totals = '' if not standalone else 'WITH population_{suffix} AS ({pop_query}) '.format(
        suffix=suffix, pop_query=pop_query)

    # For each column in our list, we add a copy of the subquery template to our long string with the column name filled in
    for col_name in col_names:
        sql_get_totals += sql_grouped_count_template.format(col_name=col_name)
    
    # We truncate the last few characters of the string to remove the final 'UNION ALL' statement
    sql_get_totals = sql_get_totals.rstrip('UNION ALL')
    return sql_get_totals


def all_counts_query(sim_pop_query, real_pop_query, col_names=None, standalone=True):
    r"""Returns SQL query to get linked value counts from a list of columns in a pair of datasets.
    
    Composes an SQL query to obtain value counts for the passed list of columns within the passed tables (defined by query),
    joined along matching column names and values. The standalone keyword argument can be set to False to use the resulting
    table as part of a larger analysis.
    
    Parameters
    ----------
    sim_pop_query : str
        The SQL Select statement for the table of simulated tumour data
    real_pop_query : str
        The SQL Select statement for the table of real tumour data
    col_names : list
        The list of table columns for which we compute value counts. Defaults to those found in SIM_AV_TUMOUR.
    standalone : Boolean, defaults to True
        Set to False to omit the final SELECT statement, so that further subqueries may be appended.
    
    Returns
    -------
    str
        An SQL query prepped for input into pd.read_sql_query
    
    """    
    # Set the default list of (non-index) column names present in the Simulacrum tumour table if no alternative list is given
    if col_names is None:
        col_names = ['GRADE', 'AGE', 'SEX', 'CREG_CODE', 'SCREENINGSTATUSFULL_CODE', 'ER_STATUS', 'ER_SCORE', 'PR_STATUS', 'PR_SCORE', 'HER2_STATUS', 'CANCERCAREPLANINTENT', 'PERFORMANCESTATUS', 'CNS', 'ACE27', 'GLEASON_PRIMARY', 'GLEASON_SECONDARY', 'GLEASON_TERTIARY', 'GLEASON_COMBINED', 'DATE_FIRST_SURGERY', 'LATERALITY', 'QUINTILE_2015', 'DIAGNOSISDATEBEST', 'SITE_ICD10_O2', 'SITE_ICD10_O2_3CHAR', 'MORPH_ICD10_O2', 'BEHAVIOUR_ICD10_O2', 'T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'STAGE_BEST_SYSTEM']

    sql_combined_totals = '''WITH population_real AS ({real_pop_query}),
population_sim AS ({sim_pop_query}),
r AS ({real_totals_query}),
s AS ({sim_totals_query}),
all_counts AS
(SELECT NVL(r.column_name, s.column_name) AS col_name, NVL(NVL(r.val, s.val), 'None') AS val,
NVL(counts_real, 0) AS counts_r, NVL(counts_sim, 0) AS counts_s
FROM r FULL OUTER JOIN s
ON (r.column_name = s.column_name AND (r.val = s.val OR (r.val IS NULL AND s.val IS NULL)))
OR (r.column_name = 'CREG_CODE' AND s.column_name = 'CREG_CODE' AND SUBSTR(r.val, 2) = SUBSTR(s.val, 2))
OR (r.column_name = 'QUINTILE_2015' AND s.column_name = 'QUINTILE_2015' AND SUBSTR(r.val, 1, 1) = SUBSTR(s.val, 1, 1)))
'''.replace('\n', ' ').format(real_pop_query=real_pop_query, 
                              sim_pop_query=sim_pop_query,
                              real_totals_query=make_totals_query(real_pop_query, col_names, 'real', standalone=False),
                              sim_totals_query=make_totals_query(sim_pop_query, col_names, 'sim', standalone=False))
    
    if standalone:
        sql_combined_totals += "SELECT * FROM all_counts"
    return sql_combined_totals
