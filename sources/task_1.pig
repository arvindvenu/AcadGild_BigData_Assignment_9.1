-- load the petroleum data set
petrol_data = LOAD '../input/petrol_dataset' USING PigStorage(',') AS (district_id:chararray, distrib_name:chararray, buy_rate:chararray, sell_rate:chararray, vol_in:int, vol_out:int, year:int);

-- project only distributor name and the volume of data sold
petrol_data_projected = FOREACH petrol_data GENERATE distrib_name, vol_out;

-- group by distributor name
petrol_data_grouped = GROUP petrol_data_projected BY distrib_name;

-- for each group(i.e. distributor name), find the sum of total volume sold
petrol_data_summed = FOREACH petrol_data_grouped GENERATE group AS distrib_name, SUM(petrol_data_projected.vol_out) AS total_volume_sold;

-- store the output on local file system
STORE petrol_data_summed INTO '../output/task_1' USING PigStorage(',');
