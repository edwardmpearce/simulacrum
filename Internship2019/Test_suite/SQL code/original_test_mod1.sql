-- Select real and simulated tables for testing
WITH
population_real AS (
SELECT * FROM AV2015.av_tumour WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E'),

population_sim AS (
SELECT * FROM analysispaulclarke.sim_av_tumour_final),

-- Obtain counts for each possible value of chosen variables in real table
counts_real as (
select count(*) as total_real, STAGE_BEST, SEX from population_real
group by STAGE_BEST, SEX),

-- Same as above but with simulated table
counts_sim as (
select count(*) as total_sim, STAGE_BEST, SEX from population_sim
group by STAGE_BEST, SEX),

-- Join the two counts tables on choice variables
joined_counts as (
select  R.STAGE_BEST, R.SEX, R.total_real, S.total_sim from counts_real R full outer join counts_sim S
on R.STAGE_BEST = S.STAGE_BEST and R.SEX = S.SEX),

-- Treat number of rows that have a particular tuple of values for the chosen variables as a binomial random variable in real and simulated tables
-- Test hypothesis that probability parameters match up at the 95 percent level of significance
-- Give pass/fail for each possible tuple
test_results as (
SELECT STAGE_BEST, SEX, total_real, total_sim, (total_sim - (select sum(total_sim) from joined_counts)*(total_real/(select sum(total_real) from joined_counts)))
/(sqrt((select sum(total_sim) from joined_counts)*(total_real/(select sum(total_real) from joined_counts))*(1 - (total_real/(select sum(total_real) from joined_counts)))))
AS test_statistic_normal_approx FROM joined_counts),

-- Count number of rows in real table that have a passing tuple of values
--Obtain overall score by dividing by total real count in the joined count table
overall_score as (
select sum_of_successes, sum_of_successes/(select sum(total_real) from joined_counts) as score_num
from (select sum(case when test_statistic_normal_approx between -1.96 and 1.96 then total_real else 0 end) as sum_of_successes from test_results))

--SELECT * FROM test_results
--ORDER BY test_statistic_normal_approx asc;
SELECT * FROM overall_score;