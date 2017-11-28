-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

states =
  FOREACH state
  GENERATE code, name;

-- Nulls have been keps in elevation as AVG does not include them in the
-- calculation so it calculates the average of known values. Population
-- nulls are 0 so that the sum is of the known values.
populated_place_data =
  FOREACH populated_place
  GENERATE state_code, (population IS NULL? 0 : population) AS population,
                        elevation;

grouped_data =
  GROUP populated_place_data BY state_code;

-- Sums population and rounds the average elevation for each state.
sum_and_average =
  FOREACH grouped_data
  GENERATE group AS state_code,
           SUM(populated_place_data.population) AS population,
           ROUND(AVG(populated_place_data.elevation)) AS elevation;

-- To get state names and data in the same table.
states_and_data =
  JOIN states BY code,
  sum_and_average BY state_code;

result =
  FOREACH states_and_data
  GENERATE name AS state_name, population, elevation;

ordered_result =
  ORDER result BY state_name;


STORE ordered_result INTO 'q2' USING PigStorage(',');
