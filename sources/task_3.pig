/*
this question was not clear. But as clarified by Support team, the problem statement is:
You have to find out the top 10 district id where 
consumption of petrol is more in last 10 years
*/
-- load the petroleum dataset
petrol_data = LOAD '../input/petrol_dataset' USING PigStorage(',') AS (district_id:chararray, distrib_name:chararray, buy_rate:chararray, sell_rate:chararray, vol_in:int, vol_out:int, year:int);

-- project only the district_id and the volume consumed and year
petrol_data_projected = FOREACH petrol_data GENERATE district_id, vol_in, year;

-- filter for those records which are for last 10 years
petrol_filtered = FILTER petrol_data_projected BY (year <= GetYear(CurrentTime()) AND (year >= GetYear(CurrentTime()) - 10));

-- group the data by district_id
petrol_data_grouped = GROUP petrol_filtered BY district_id;

-- find the total volume consumed per group(i.e district_id)
petrol_data_summed = FOREACH petrol_data_grouped GENERATE group AS district_id, SUM(petrol_filtered.vol_in) AS total_vol_consumed;

-- sort the data in descending order of volume consumed
petrol_data_sorted = ORDER petrol_data_summed BY total_vol_consumed DESC;

-- to select the top 10 district_ids limit the sorted relation to only 10 tuples
petrol_data_top_10 = LIMIT petrol_data_sorted 10;

-- store the top 10 district ids in local file system
STORE petrol_data_top_10 INTO '../output/task_3' USING PigStorage(',');
