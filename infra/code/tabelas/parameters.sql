CREATE EXTERNAL TABLE companyretail_datalake_dev_raw.parameters(
  interfacegroup varchar(65535) , 
  tabledatalake varchar(65535) , 
  tableflatfile varchar(65535) , 
  source varchar(65535) , 
  mode varchar(65535) , 
  ispartition varchar(65535) , 
  partitioncolumn varchar(65535) )
ROW FORMAT  serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://companyretail-datalake-dev-code/parameters'
