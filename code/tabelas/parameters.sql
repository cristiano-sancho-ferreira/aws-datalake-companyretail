CREATE EXTERNAL TABLE csancho_datalake_raw_dev.parameters (
InterfaceGroup varchar(255),
TableDataLake varchar(255),
TableFlatFile varchar(255),
Source varchar(10),
Mode varchar(10),
IsPartition varchar(1),
PartitionColumn varchar(255))
ROW FORMAT  serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://csancho-datalake-dev-code/parameters'

CREATE EXTERNAL TABLE `parameters`(
  `interfacegroup` varchar(65535) COMMENT 'from deserializer', 
  `tabledatalake` varchar(65535) COMMENT 'from deserializer', 
  `tableflatfile` varchar(65535) COMMENT 'from deserializer', 
  `source` varchar(65535) COMMENT 'from deserializer', 
  `mode` varchar(65535) COMMENT 'from deserializer', 
  `ispartition` varchar(65535) COMMENT 'from deserializer', 
  `partitioncolumn` varchar(65535) COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION
  's3://csancho-datalake-glue-script/parameters'
TBLPROPERTIES (
  'transient_lastDdlTime'='1630211595')
