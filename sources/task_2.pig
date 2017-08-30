-- correction as clarified by the support team 
/*
orignal question:Which are the top 10 distributers ID's for selling petrol? 
Also display the amount of petrol sold in volume.

corrected question: Which are the top 10 district ID's for selling petrol? 
Also display the amount of petrol sold in volume. 
*/


-- load the petroleum dataset
petrol_data = LOAD '../input/petrol_dataset' USING PigStorage(',') AS (district_id:chararray, distrib_name:chararray, buy_rate:chararray, sell_rate:chararray, vol_in:int, vol_out:int, year:int);

-- project only the district_id and the volume sold
petrol_data_projected = FOREACH petrol_data GENERATE district_id, vol_out;

/*
To find the total amount of petrol sold per district first group by
district_id and then sum the vol_out for each group
*/
-- first group the data by district_id
petrol_data_grouped = GROUP petrol_data_projected BY district_id;
 
-- sum vol_out for each group(i.e. each district_id)
petrol_data_summed = FOREACH petrol_data_grouped GENERATE group AS district_id, SUM(petrol_data_projected.vol_out) AS total_volume_sold;

-- sort the summed data in descending order of volume sold
petrol_data_sorted = ORDER petrol_data_summed BY total_volume_sold DESC;

-- to select the top 10 district_ids limit the sorted relation to only 10 tuples
petrol_data_top_10 = LIMIT petrol_data_sorted 10;

-- store the top 10 district ids in local file system
STORE petrol_data_top_10 INTO '../output/task_2' USING PigStorage(',');
