-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

states =
  FOREACH state
  GENERATE code, name AS state_name;

-- Replaces null population values with 0s.
populated_places =
  FOREACH populated_place
  GENERATE name, state_code, population;

-- Names of states and rest of data are in the same table.
states_and_places =
  JOIN states BY code,
  populated_places BY state_code;

-- Generates a table with relevant information.
states_with_data =
  FOREACH states_and_places
  GENERATE state_name, name, population;

-- Removes the null population values as these mean nothing
-- in the context.
states_without_nulls =
  FILTER states_with_data
  BY population IS NOT NULL;

grouped =
  GROUP states_without_nulls BY state_name;

-- Orders by population and then limits.
-- Need to flatten as limited is a group.
result =
  FOREACH grouped {
    ordered =
      ORDER states_without_nulls BY population DESC;
     limited =
      LIMIT ordered 5;
      GENERATE FLATTEN(limited);
  }

-- Orders result by name of state and then population.
-- If population is the same, it orders by county name.
ordered_result =
  ORDER result BY state_name ASC, population DESC, name ASC;


STORE ordered_result INTO 'q4' USING PigStorage(',');
