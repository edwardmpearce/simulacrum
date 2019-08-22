#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This file contains the following module parameters:
    * params - Connection parameters for the database.connect function
    * col_names - A list of the non-index columns present in SIM_AV_TUMOUR in the Simulacrum. These are the options for the col_name function parameter.
    * col_name_pairs - List of pairs of non-index columns present in SIM_AV_TUMOUR in the Simulacrum.
    * plot_params_dict - A dictionary of plotting parameters used by the plots.plot_by_category function. The keys are plot_type options and the values are dictionaries of plotting parameters.
"""

# Parameters for the database.connect function
params = dict(dialect='oracle',
              driver='cx_oracle',
              server='localhost',
              port=1523,
              database='casref01')


# Hard-coded list of non-index columns present in SIM_AV_TUMOUR in the Simulacrum. These are the options for the col_name function parameter.
col_names = ['GRADE', 'AGE', 'SEX', 'CREG_CODE', 'SCREENINGSTATUSFULL_CODE', 'ER_STATUS', 'ER_SCORE', 'PR_STATUS', 'PR_SCORE', 'HER2_STATUS', 'CANCERCAREPLANINTENT', 'PERFORMANCESTATUS', 'CNS', 'ACE27', 'GLEASON_PRIMARY', 'GLEASON_SECONDARY', 'GLEASON_TERTIARY', 'GLEASON_COMBINED', 'DATE_FIRST_SURGERY', 'LATERALITY', 'QUINTILE_2015', 'DIAGNOSISDATEBEST', 'SITE_ICD10_O2', 'SITE_ICD10_O2_3CHAR', 'MORPH_ICD10_O2', 'BEHAVIOUR_ICD10_O2', 'T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'STAGE_BEST_SYSTEM']


# List of pairs of non-index columns present in SIM_AV_TUMOUR in the Simulacrum.
col_name_pairs = [(col_names[i], col_names[j]) for i in range(len(col_names)) for j in range(i+1, len(col_names))]


# Hard-coded dictionary of plotting parameters used by the plot_by_category function. 
# The keys are plot_type options and the values are dictionaries of plotting parameters.
plot_params_dict = {'Counts': 
                    {'y_values': ['counts_r', 'counts_s'],
                    'labels': ['Real', 'Simulated'],
                    'x_label': 'Category',
                    'y_label': 'Counts',
                    'title': 'Counts by Category in {}'},
                'Proportions': 
                    {'y_values': ['proportion_r', 'proportion_s'],
                    'labels': ['Real', 'Simulated'],
                    'x_label': 'Category',
                    'y_label': 'Proportion',
                    'title': 'Proportion by Category in {}'},
                'Absolute Difference': 
                    {'y_values': ['abs_diff'],
                    'labels': ['$p_{sim} - p_{real}$'],
                    'x_label': 'Category',
                    'y_label': 'Difference in Proportions',
                    'title': 'Difference in Proportions by Category in {}'},
                'Relative Difference': 
                    {'y_values': ['rel_diff'],
                    'labels': ['$(p_{sim} - p_{real})/p_{real}$'],
                    'x_label': 'Category',
                    'y_label': 'Relative Difference in Proportions',
                    'title': 'Relative Difference in Proportions by Category in {}'},
                'One-sample Binomial z-test': 
                    {'y_values': ['binom_z_test_one_sample'],
                    'labels': ['$z = (X - np)/\sqrt{np(1-p)}$'],
                    'x_label': 'Category',
                    'y_label': 'z-test statistic',
                    'title': 'Binomial One-sample z-test statistic by Category in {}'},
                'Pooled Two-sample Binomial z-test': 
                    {'y_values': ['z_test_two_sample_pooled'],
                    'labels': ['$z = (p_{1}-p_{2})/\sqrt{\hat{p}(1-\hat{p})(1/n_{1}+1/n_{2})}$'],
                    'x_label': 'Category',
                    'y_label': 'z-test statistic',
                    'title': 'Pooled Two-sample Binomial z-test statistic by Category in {}'},
                'Pearson Chi-squared test summand': 
                    {'y_values': ['pearson_summand'],
                    'labels': ['$(O - E)^{2}/E$'],
                    'x_label': 'Category',
                    'y_label': 'Pearson test summand',
                    'title': 'Pearson Chi-squared test summand by Category in {}'},
                'Likelihood-ratio test summand': 
                     {'y_values': ['lr_summand'],
                    'labels': ['$O * \ln(O/E)$'],
                    'x_label': 'Category',
                    'y_label': 'Likelihood-ratio test summand',
                    'title': 'Likelihood-ratio test summand by Category in {}'}
                    }
