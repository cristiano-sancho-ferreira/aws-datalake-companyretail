#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_lkvta_pos_register.sh
## Autor      : Jackson.Silva(ST IT Consulting)
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              10/07/2018| Código inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="user"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.lkvta_pos_register_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.lkvta_pos_register;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.lkvta_pos_register_flatfile(
  pos_register_id varchar(10), 
  pos_register_type_id varchar(10), 
  pos_register_desc varchar(255), 
  store_department_id int, 
  department_dedicated_ind varchar(3), 
  pos_equip_id int, 
  position_num varchar(50), 
  chain_cd varchar(50), 
  location_id varchar(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/lkvta_pos_register';
    \p \echo \echo   


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.lkvta_pos_register(
  pos_register_id varchar(10), 
  pos_register_type_id varchar(10), 
  pos_register_desc varchar(255), 
  store_department_id int,  
  department_dedicated_ind varchar(3),
  pos_equip_id int, 
  position_num varchar(50), 
  chain_cd varchar(50), 
  location_id varchar(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/lkvta_pos_register'
    \p \echo \echo 
    
EOF
