-- DONT CHANGE
DROP TABLE IF EXISTS {target_db}.{kpd_table_name};
CREATE EXTERNAL TABLE IF NOT EXISTS {target_db}.{kpd_table_name}
(
ctl_loading int,
ctl_validfrom string,
partitions string,
is_force_load int,
last_success_step int,
last_step_in_general int,
count_rows bigint,
is_change int
)
STORED AS PARQUET
LOCATION '/data/custom/salesntwrk/{target_path}/pa/{kpd_table_name}'
--LOCATION '/user/hive/warehouse/{target_db}.db/{target_table}'