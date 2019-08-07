--Select real and simulated tables for testing

with population_real as (
select * from AV2015.av_tumour WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E'),

population_sim as (
select * from analysispaulclarke.sim_av_tumour_final),


--Obtain counts for each possible value of chosen variables in real table

counts_real as (
select count(*) as total_real, STAGE_BEST, SEX from population_real
group by STAGE_BEST, SEX),


--Same as above but with simulated table

counts_sim as (
select count(*) as total_sim, STAGE_BEST, SEX from population_sim
group by STAGE_BEST, SEX),


--Join the two counts tables on choice variables

joined_counts as (
select  R.STAGE_BEST, R.SEX, R.total_real, S.total_sim from counts_real R full outer join counts_sim S
on R.STAGE_BEST = S.STAGE_BEST and R.SEX = S.SEX)


--Treat number of rows that have a particular tuple of values for the chosen variables as a binomial random variable in real and simulated tables
--Test hypothesis that probability parameters match up at the 95 percent level of significance
--Give pass/fail for each possible tuple
--Count number of rows in real table that have a passing tuple of values
SELECT STAGE_BEST, SEX, total_sim - (select sum(total_sim) from joined_counts)*(total_real/(select sum(total_real) from joined_counts))
/(sqrt((select sum(total_sim) from joined_counts)*(total_real/(select sum(total_real) from joined_counts))*(1 - (total_real/(select sum(total_real) from joined_counts))))) 
FROM joined_counts;


success_sum as (
select sum(case when (total_sim - (select sum(total_sim) from joined_counts)*(total_real/(select sum(total_real) from joined_counts)))
/(sqrt((select sum(total_sim) from joined_counts)*(total_real/(select sum(total_real) from joined_counts))*(1 - (total_real/(select sum(total_real) from joined_counts))))) 
between -1.96 and 1.96
then total_real else 0 end) as sum_of_successes
from joined_counts),


--Obtain overall score by dividing by total real count in the joined count table

overall_score as (
select sum_of_successes/(select sum(total_real) from joined_counts) as score_num
from success_sum)

SELECT * FROM joined_counts;

SELECT * FROM success_sum;

SELECT * FROM overall_score;
