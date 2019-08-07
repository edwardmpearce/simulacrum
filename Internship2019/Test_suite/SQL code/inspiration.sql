WITH 
population_sim AS (SELECT * FROM analysispaulclarke.sim_av_tumour_final),

-- Gets column names from the simulated tumour table, excluding index columns by requiring the number of distinct entries in the column to be fewer than 9000, but retaining dates
column_data AS (SELECT column_name FROM all_tab_cols WHERE owner = 'ANALYSISPAULCLARKE' AND table_name = 'SIM_AV_TUMOUR_FINAL' AND num_distinct < 9000),

-- Create a table of strings containing parts of a large SQL query. After modifying the final line, this query collects category sizes for each column. 
-- Double pipe || concatenates, two single-quotes '' escapes to one single-quote.
agg_queries AS (
SELECT 
('SELECT ''' || column_name || ''' AS column_name, TO_CHAR(' || column_name || ') AS category, COUNT(*) AS counts_sim FROM population_sim GROUP BY ' || column_name || ' UNION ALL') AS line
FROM column_data), 

-- As the desired output is a query string for later use, this might as well be composed in Python or R, which could tidy and the run the query, avoiding the need to copy & paste

-- Create a second aggregation query string for the real data, run it, outer join the results, filling null values with zeros for zero counts. 
-- Create columns for category proportions (divide by corresponding dataset size), column for binomial test results: z-value and pass or fail at different significance levels