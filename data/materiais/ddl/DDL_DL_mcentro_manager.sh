#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_mcentro_manager.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.mcentro_manager_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.mcentro_manager;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.mcentro_manager_flatfile(
Location_Manager_Cd	VARCHAR(50),
Country_Cd	VARCHAR(50),
Location_Manager_Name	VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/mcentro_manager';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.mcentro_manager(
Location_Manager_Cd	VARCHAR(50),
Country_Cd	VARCHAR(50),
Location_Manager_Name	VARCHAR(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/mcentro_manager' 
    \p \echo \echo 
    
EOF
