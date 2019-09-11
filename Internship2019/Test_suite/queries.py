#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file can be imported as a module and contains the following functions:
    * get_cols_query - returns an SQL query to get the column names of a table in a database
    * make_totals_query - returns an SQL query to get counts of values in a list of columns
    * all_counts_query - returns an SQL query to get linked value counts from a list of columns in a pair of datasets.
    * compute_stats_query - returns an SQL query to compute statistics based on value counts from a pair of datasets.
"""


from params import col_names, col_name_pairs


__author__ = 'Edward Pearce'
__copyright__ = 'Copyright 2019, Simulacrum Test Suite'
__credits__ = ['Edward Pearce']
__license__ = 'MIT'
__version__ = '1.0.0'
__maintainer__ = 'Edward Pearce'
__email__ = 'edward.pearce@phe.gov.uk'
__status__ = 'Production'


def get_cols_query(owner, table, condition=""):
    r"""Returns SQL query to get column names from a given table where additional conditions may be specified"""
    query = '''SELECT column_name FROM all_tab_cols WHERE owner = '{}' AND table_name = '{}' '''.format(owner, table)
    if condition != "":
        query = query + "AND {}".format(condition)
    return query


def make_totals_query(pop_query, suffix='', field_list=None, num_variates=1, standalone=True):
    r"""If `num_variates` == 1: Compose a large SQL query to obtain counts of values in a list of columns.
    Concatenates subqueries which obtain value counts for the passed columns within the passed table (defined by query).
    
    If `num_variates` == 2: Compose a very large SQL query to obtain counts of value pairs over pairs of fields in a table.
    Concatenates subqueries which obtain counts of value pairs for distinct pairs of columns within the passed table (defined by query).
    
    Parameters
    ----------
    pop_query : str
        The SQL Select statement to obtain the table for which we want to find aggregate information
    suffix : str, optional
        A suffix added to the 'counts' column name in the final output
    num_variates : int, defaults to 1
        Select whether counts are grouped by 1 variable or 2 variables (over all distinct combinations). 
    standalone : Boolean, defaults to True
        Set to False if the output will be a subquery, in order to avoid nested WITH statements.
    
    Returns
    -------
    str
        An SQL query prepped for input into pd.read_sql_query
    
    """    
    # This is the template for the subquery obtaining group counts, 
    # along with a 'UNION ALL' statement to join the smaller tables together
    if num_variates == 1:
        template = '''SELECT
'{col_name}' AS column_name,
NVL(TO_CHAR({col_name}), 'None') AS val,
COUNT(*) AS counts_{suffix}
FROM population_{suffix}
GROUP BY {col_name}
UNION ALL
'''.replace('\n', ' ').replace('{suffix}', suffix)

    elif num_variates == 2:
        template = '''SELECT
'{col_name1}' AS column_name1,
'{col_name2}' AS column_name2,
NVL(TO_CHAR({col_name1}), 'None') AS val1,
NVL(TO_CHAR({col_name2}), 'None') AS val2,
COUNT(*) AS counts_{suffix}
FROM population_{suffix}
GROUP BY {col_name1}, {col_name2}
UNION ALL
'''.replace('\n', ' ').replace('{suffix}', suffix)
    else:
        print('The keyword `num_variates` currently only accepts values in [1,2]')  
        return ''
    
    # Here we initiliaze our long string of SQL code
    sql = '' if not standalone else 'WITH population_{suffix} AS ({pop_query}) '.format(suffix=suffix, pop_query=pop_query)
    
    if num_variates == 1:
        # Load the list of (non-index) column names present in the Simulacrum tumour table
        # For each column in our list, we add a copy of the subquery template to our long string with the column name filled in
        iterator = field_list if field_list is not None else col_names
        for col_name in iterator:
            sql += template.format(col_name=col_name)

    elif num_variates == 2:
        # Load the list of distinct pairs of (non-index) column names present in the Simulacrum tumour table
        # For each pair of columns in our list, we add a copy of the subquery template to our long string with the column names filled in
        iterator = field_list if field_list is not None else col_name_pairs
        for pair in iterator:
            sql += template.format(col_name1=pair[0], col_name2=pair[1])
    
    # We truncate the last few characters of the string to remove the final 'UNION ALL' statement
    sql = sql.rstrip('UNION ALL')
    return sql


def all_counts_query(sim_pop_query, real_pop_query, standalone=True):
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
    standalone : Boolean, defaults to True
        Set to False to omit the final SELECT statement, so that further subqueries may be appended.
    
    Returns
    -------
    str
        An SQL query prepped for input into pd.read_sql_query
    
    """    
    sql_combined_totals = '''WITH population_real AS ({real_pop_query}),
population_sim AS ({sim_pop_query}),
r AS ({real_totals_query}),
s AS ({sim_totals_query}),
all_counts AS
(SELECT 
NVL(r.column_name, s.column_name) AS col_name,
COALESCE(r.val, s.val, 'None') AS val,
NVL(counts_real, 0) AS counts_r,
NVL(counts_sim, 0) AS counts_s
FROM r FULL OUTER JOIN s
ON r.column_name = s.column_name AND NVL(r.val, 'None') = NVL(s.val, 'None'))
'''.replace('\n', ' ').format(real_pop_query=real_pop_query, 
                              sim_pop_query=sim_pop_query,
                              real_totals_query=make_totals_query(real_pop_query, 'real', standalone=False),
                              sim_totals_query=make_totals_query(sim_pop_query, 'sim', standalone=False))
    
    if standalone:
        sql_combined_totals += "SELECT * FROM all_counts"
    return sql_combined_totals


def compute_stats_query(sim_pop_query, real_pop_query, standalone=True):
    r"""Returns SQL query to compute statistics based on value counts from a pair of datasets.
    
    Composes an SQL query to obtain value counts, compute test statistics for the passed list of columns within the passed tables (defined by query),
    joined along matching column names and values. 
    
    Parameters
    ----------
    sim_pop_query : str
        The SQL Select statement for the table of simulated tumour data
    real_pop_query : str
        The SQL Select statement for the table of real tumour data
    standalone : Boolean, defaults to True
        Set to False to omit the final SELECT statement, so that further subqueries may be appended.
    
    Returns
    -------
    str
        An SQL query prepped for input into pd.read_sql_query
    
    """    
    analysis_subquery = '''pop_sizes AS (SELECT * FROM 
(SELECT COUNT(*) AS total_real FROM population_real), 
(SELECT COUNT(*) AS total_sim FROM population_sim)),
proportions AS
(SELECT col_name, val, counts_r, counts_s,
counts_r/total_real AS proportion_r,
counts_s/total_sim AS proportion_s,
counts_s/total_sim - counts_r/total_real AS abs_diff,
CASE WHEN counts_r = 0 THEN NULL
    ELSE (counts_s * total_real)/(counts_r * total_sim) - 1
    END AS rel_diff,
(counts_r + counts_s)/(total_real + total_sim) AS average
FROM all_counts, pop_sizes),
results AS
(SELECT col_name, val, counts_r, counts_s, proportion_r, proportion_s, abs_diff, rel_diff, average,
CASE WHEN proportion_r = 0 THEN NULL
    ELSE (counts_s - total_sim * proportion_r)/SQRT(total_sim * proportion_r * (1 - proportion_r))
    END AS binom_z_test_one_sample,
CASE WHEN proportion_r = 0 THEN NULL
    WHEN (total_sim > 9 * (1 - proportion_r)/proportion_r AND total_sim > 9 * proportion_r / (1 - proportion_r)) THEN 1
    ELSE 0
    END AS one_sample_z_approx_valid,
abs_diff/SQRT(average * (1 - average) * ((total_real + total_sim)/(total_real * total_sim))) 
AS z_test_two_sample_pooled,
CASE WHEN counts_r = 0 THEN 2*counts_s*counts_s
    ELSE (counts_s - total_sim * proportion_r)*(counts_s - total_sim * proportion_r)/(total_sim * proportion_r)
    END AS Pearson_summand,
CASE WHEN counts_s = 0 THEN 0
    WHEN counts_r = 0 THEN counts_s * LOG(EXP(1), 2 * counts_s)
    ELSE counts_s * LOG(EXP(1), proportion_s/proportion_r)
    END AS LR_summand
FROM proportions, pop_sizes)
'''.replace('\n', ' ')
        
    if standalone:
        analysis_subquery += "SELECT * FROM results"

    complete_query = all_counts_query(sim_pop_query, real_pop_query, standalone=False) + ',' + analysis_subquery
    return complete_query
