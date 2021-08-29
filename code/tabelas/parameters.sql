CREATE EXTERNAL TABLE csancho_datalake_raw_dev.parameters (
InterfaceGroup varchar(255),
TableDataLake varchar(255),
TableFlatFile varchar(255),
Source varchar(10),
Mode varchar(10),
IsPartition varchar(1),
PartitionColumn varchar(255))
ROW FORMAT  serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://csancho-datalake-glue-script/parameters'
