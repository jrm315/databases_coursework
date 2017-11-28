-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Loads the relevant columns from features.
features =
  FOREACH feature
  GENERATE state_name, county, type;

-- Groups features as there are different types to
-- each state and county pair.
grouped_features =
  GROUP features BY (state_name, county);

-- Makes two tables with only stream types
-- and ppl types to allow for counting.
feature_types =
  FOREACH grouped_features {
    stream_type =
      FILTER features
      BY type == 'stream';
    ppl_type =
      FILTER features
      BY type == 'ppl';
    GENERATE group.state_name AS state_name,
             group.county AS county,
             COUNT(ppl_type.state_name) AS no_ppl,
             COUNT(stream_type.state_name) AS no_stream;
  }

-- Removes the entries with null counties associated with states.
no_nulls =
  FILTER feature_types
  BY county != 'null';

-- Orders the data as required.
result =
  ORDER no_nulls
  BY state_name, county;

STORE result INTO 'q3' USING PigStorage(',');
