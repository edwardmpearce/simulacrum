#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file can be imported as a module and contains the following functions:
    * get_cols_query - returns an SQL query to get the column names of a table in a database
    * make_totals_query - returns an SQL query to get counts of values in a list of columns
    * all_counts_query - returns an SQL query to get linked value counts from a list of columns in a pair of datasets.
    * compute_stats_query - returns an SQL query to compute statistics based on value counts from a pair of datasets.
    * chi2_query - returns an SQL query to compute chi-squared statistics between fields in a pair of datasets.
This file also contains the following module parameters:
    * AV2015_pop_query - A string containing an SQL query to construct a table of tumour data from the AV2015 snapshot  
    * AV2017_pop_query - A string containing an SQL query to construct a table of tumour data from the AV2017 snapshot  
"""


from params import col_names, col_name_pairs


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


def make_totals_query(pop_query, suffix='', num_variates=1, standalone=True):
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
COUNT(*) AS paired_counts_{suffix}
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
        for col_name in col_names:
            sql += template.format(col_name=col_name)
    
    elif num_variates == 2:
        # Load the list of distinct pairs of (non-index) column names present in the Simulacrum tumour table
        # For each pair of columns in our list, we add a copy of the subquery template to our long string with the column names filled in
        for pair in col_name_pairs:
            sql += template.format(col_name1=pair[0], col_name2=pair[1])
    
    # We truncate the last few characters of the string to remove the final 'UNION ALL' statement
    sql = sql.rstrip('UNION ALL')
    return sql


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


# A string containing an SQL query to construct a table of tumour data from the AV2015 snapshot
# ready for comparison with its counterpart in the Simulacrum
AV2015_pop_query = '''SELECT multi_depr_index.QUINTILE_2015, av_tumour.GRADE, av_tumour.AGE, av_tumour.SEX, av_tumour.CREG_CODE, av_tumour.SCREENINGSTATUSFULL_CODE, av_tumour.ER_STATUS, av_tumour.ER_SCORE, av_tumour.PR_STATUS, av_tumour.PR_SCORE, av_tumour.HER2_STATUS, av_tumour.LATERALITY, av_tumour.DIAGNOSISDATEBEST, av_tumour.SITE_ICD10_O2, av_tumour.SITE_ICD10_O2_3CHAR, av_tumour.MORPH_ICD10_O2, av_tumour.BEHAVIOUR_ICD10_O2, av_tumour.T_BEST, av_tumour.N_BEST, av_tumour.M_BEST, av_tumour.STAGE_BEST, av_tumour.STAGE_BEST_SYSTEM, av_tumour_exp.CANCERCAREPLANINTENT, av_tumour_exp.PERFORMANCESTATUS, av_tumour_exp.CNS, av_tumour_exp.ACE27, av_tumour_exp.DATE_FIRST_SURGERY, av_tumour_exp.GLEASON_PRIMARY, av_tumour_exp.GLEASON_SECONDARY, av_tumour_exp.GLEASON_TERTIARY, av_tumour_exp.GLEASON_COMBINED
FROM
(SELECT TUMOURID, LSOA11_CODE, GRADE, AGE, SEX, CREG_CODE, SCREENINGSTATUSFULL_CODE, ER_STATUS, ER_SCORE, PR_STATUS, PR_SCORE, HER2_STATUS, LATERALITY, DIAGNOSISDATEBEST, SITE_ICD10_O2, SITE_ICD10_O2_3CHAR, MORPH_ICD10_O2, BEHAVIOUR_ICD10_O2, T_BEST, N_BEST, M_BEST, STAGE_BEST, STAGE_BEST_SYSTEM
FROM AV2015.AV_TUMOUR
WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1) av_tumour
LEFT JOIN
(SELECT TUMOURID, CANCERCAREPLANINTENT, PERFORMANCESTATUS, CNS, ACE27, DATE_FIRST_SURGERY, GLEASON_PRIMARY, GLEASON_SECONDARY, GLEASON_TERTIARY, GLEASON_COMBINED 
FROM AV2015.AV_TUMOUR_EXPERIMENTAL_1612) av_tumour_exp
ON av_tumour.tumourid = av_tumour_exp.tumourid
LEFT JOIN IMD.ID2015 multi_depr_index
ON av_tumour.LSOA11_CODE = multi_depr_index.LSOA11_CODE
'''


# A string containing an SQL query to construct a table of tumour data from the AV2017 snapshot 
# ready for comparison with its counterpart in the Simulacrum
AV2017_pop_query = '''SELECT multi_depr_index.QUINTILE_2015, at_tumour.GRADE, at_tumour.AGE, at_tumour.SEX, at_tumour.CREG_CODE, at_tumour.SCREENINGSTATUSFULL_CODE, at_tumour.ER_STATUS, at_tumour.ER_SCORE, at_tumour.PR_STATUS, at_tumour.PR_SCORE, at_tumour.HER2_STATUS, at_tumour.GLEASON_PRIMARY, at_tumour.GLEASON_SECONDARY, at_tumour.GLEASON_TERTIARY, at_tumour.GLEASON_COMBINED, at_tumour.LATERALITY, at_tumour.DIAGNOSISDATEBEST, at_tumour.SITE_ICD10_O2, at_tumour.SITE_ICD10_O2_3CHAR, at_tumour.MORPH_ICD10_O2, at_tumour.BEHAVIOUR_ICD10_O2, at_tumour.T_BEST, at_tumour.N_BEST, at_tumour.M_BEST, at_tumour.STAGE_BEST, at_tumour.STAGE_BEST_SYSTEM, at_tumour_exp.CANCERCAREPLANINTENT, at_tumour_exp.PERFORMANCESTATUS, at_tumour_exp.CNS, at_tumour_exp.ACE27, at_tumour_exp.DATE_FIRST_SURGERY 
FROM
(SELECT TUMOURID, LSOA11_CODE, GRADE, AGE, SEX, CREG_CODE, SCREENINGSTATUSFULL_CODE, ER_STATUS, ER_SCORE, PR_STATUS, PR_SCORE, HER2_STATUS, GLEASON_PRIMARY, GLEASON_SECONDARY, GLEASON_TERTIARY, GLEASON_COMBINED, LATERALITY, DIAGNOSISDATEBEST, SITE_ICD10_O2, SITE_ICD10_O2_3CHAR, MORPH_ICD10_O2, BEHAVIOUR_ICD10_O2, T_BEST, N_BEST, M_BEST, STAGE_BEST, STAGE_BEST_SYSTEM
FROM AV2017.AT_TUMOUR_ENGLAND 
WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2017') AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1) at_tumour
LEFT JOIN 
(SELECT TUMOURID, CANCERCAREPLANINTENT, PERFORMANCESTATUS, CNS, ACE27, DATE_FIRST_SURGERY 
FROM AV2017.AT_TUMOUR_EXPERIMENTAL_ENGLAND) at_tumour_exp
ON at_tumour.tumourid = at_tumour_exp.tumourid
LEFT JOIN IMD.ID2015 multi_depr_index
ON at_tumour.LSOA11_CODE = multi_depr_index.LSOA11_CODE
'''
