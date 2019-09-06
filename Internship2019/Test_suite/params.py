#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file contains the following module parameters:
    * params - Connection parameters for the database.connect function
    * key_list - Aliases for tables of CAS/Simulacrum data. Frequently used as keys in dictionary data structures throughout
    * comparison_pairs - Pairs of keys from key_list representing pairs of tables which will be compared. Used as keys for comparison tables and plots
    * filepath_dictionary - Contains names of filepaths where local copies of group counts data can be stored
    
Column name related parameters, mostly encapsulated in the variable `field_list_dict` which stores various lists of column names and pairs of column names:
    * categorical_cols - A list of non-index column names for categorical/discrete value fields in SIM_AV_TUMOUR, plus two derived categorical fields.
    * categorical_col_pairs - Ordered pairs of distinct category field names, without symmetry. Order in pairs based order of field name in `categorical_cols`.
    * date_cols - A list of column names for date value fields in SIM_AV_TUMOUR.
Other parameters:
    * col_names - The concatenation of the two lists `categorical_cols` and `date_cols`.
    * col_name_pairs - List of pairs of non-index columns present in SIM_AV_TUMOUR in the Simulacrum.
"""

# Parameters for the database.connect function
params = dict(dialect='oracle',
              driver='cx_oracle',
              server='localhost',
              port=1523,
              database='casref01')


# Aliases for tables of CAS/Simulacrum data
key_list = ['sim1', 'sim2', 'av2015', 'av2017'] # Same values as pop_queries.keys()
# Pairs of tables which will be compared
comparison_pairs = [('sim1', 'av2015'), ('sim2', 'av2017')]


# Lists of non-index columns present in SIM_AV_TUMOUR in the Simulacrum, plus derived fields, sorted by data type.
categorical_cols = ['QUINTILE_2015', 'CREG_CODE', 'GRADE', 'SEX', 'SITE_ICD10_O2', 'SITE_ICD10_O2_3CHAR', 'MORPH_ICD10_O2', 'BEHAVIOUR_ICD10_O2', 'T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'STAGE_BEST_SYSTEM', 'SCREENINGSTATUSFULL_CODE', 'ER_STATUS', 'ER_SCORE', 'PR_STATUS', 'PR_SCORE', 'HER2_STATUS', 'LATERALITY', 'GLEASON_PRIMARY', 'GLEASON_SECONDARY', 'GLEASON_TERTIARY', 'GLEASON_COMBINED', 'CANCERCAREPLANINTENT', 'PERFORMANCESTATUS', 'CNS', 'ACE27', 'DIAGNOSISMONTHBEST', 'MONTH_FIRST_SURGERY', 'AGE']

categorical_col_pairs = [(categorical_cols[i], categorical_cols[j]) 
                         for i in range(len(categorical_cols)) 
                         for j in range(i+1, len(categorical_cols))]

date_cols = ['DIAGNOSISDATEBEST', 'DATE_FIRST_SURGERY']

field_list_dict = {'univariate_categorical': categorical_cols,
                   'univariate_dates': date_cols,
                   'bivariate_categorical': categorical_col_pairs,
                   'categorical_cross_diagnosis_date': [(categorical_col, 'DIAGNOSISDATEBEST') for categorical_col in categorical_cols],
                   'categorical_cross_surgery_date': [(categorical_col, 'DATE_FIRST_SURGERY') for categorical_col in categorical_cols],
                   'surgery_date_cross_diagnosis_date': [('DATE_FIRST_SURGERY', 'DIAGNOSISDATEBEST')]
                   }

# List of non-index columns present in SIM_AV_TUMOUR in the Simulacrum.
col_names = categorical_cols + date_cols

# List of pairs of non-index columns present in SIM_AV_TUMOUR in the Simulacrum.
col_name_pairs = [(col_names[i], col_names[j]) for i in range(len(col_names)) for j in range(i+1, len(col_names))]


# File names used for storing, retrieving local copies of grouped counts data extracted from the SQL database
filepath_templates = {'univariate_categorical': r"results\{}_univariate_categorical.csv",
                      'univariate_dates': r"results\{}_univariate_dates.csv",
                      'bivariate_categorical': r"results\{}_bivariate_categorical.csv",
                      'categorical_cross_diagnosis_date': r"results\{}_categorical_cross_diagnosis_dates.csv",
                      'categorical_cross_surgery_date': r"results\{}_categorical_cross_surgery_dates.csv",
                      'surgery_date_cross_diagnosis_date': r"results\{}_bivariate_counts_double_dates.csv"
                      }

filepath_dictionary = {count_type: {key: template.format(key.upper()) for key in ['sim1', 'sim2', 'av2015', 'av2017']} for count_type, template in filepath_templates.items()}

