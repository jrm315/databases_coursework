-- Load the state, feature and populated_place tables
RUN /vol/automed/data/usgs/load_tables.pig

-- Project just the columns of feature we need later
feature_data =
   FOREACH feature
   GENERATE name AS feature_name,latitude,longitude;

-- Project just the columns of populated_place we need later
populated_place_data =
   FOREACH populated_place
   GENERATE name AS populated_place_name,latitude,longitude;

-- Find those features that match the latitude and longitude of a place
same_place =
   JOIN feature_data BY (latitude,longitude),
        populated_place_data BY (latitude,longitude);

-- Get rid of data not required for result
same_place_data =
   FOREACH same_place
   GENERATE feature_name, populated_place_name, 
        feature_data::latitude, feature_data::longitude;

-- Provide the result sorted
sorted_same_place_data =
   ORDER same_place_data
   BY    latitude,longitude;

STORE sorted_same_place_data INTO 'q0' USING PigStorage(',');
