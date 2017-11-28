-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Changes features names to all upper class and
-- extracts only that.
feature_names =
  FOREACH feature
  GENERATE UPPER(state_name) AS feature_name;

-- Removes duplicates.
unique_feature_names =
  DISTINCT feature_names;

-- Extracts names of states.
state_names =
  FOREACH state
  GENERATE name AS state_name;

-- Left joins features and states as part of a DIFF,
-- unique values in feature will have a null as a
-- state does not exist.
feature_and_state =
  JOIN unique_feature_names BY feature_name LEFT,
  state_names BY state_name;

-- Removes features with null states.
feature_without_state =
  FILTER feature_and_state
  BY state_names::state_name IS NULL;

result =
  FOREACH feature_without_state
  GENERATE feature_name;

-- Orders as required.
ordered_result =
  ORDER result
  BY feature_name;

STORE ordered_result INTO 'q1' USING PigStorage(',');
