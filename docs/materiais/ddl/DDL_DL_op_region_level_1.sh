#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_op_region_level_1.sh
## Autor      : Silva
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.op_region_level_1_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.op_region_level_1;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.op_region_level_1_flatfile(
Country_Cd	VARCHAR(50),
Op_Region_Level_1_Cd	VARCHAR(50),
Op_Region_Level_0_Cd	VARCHAR(50),
Op_Region_Level_1_Desc	VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/op_region_level_1';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.op_region_level_1(
Country_Cd	VARCHAR(50),
Op_Region_Level_1_Cd	VARCHAR(50),
Op_Region_Level_0_Cd	VARCHAR(50),
Op_Region_Level_1_Desc	VARCHAR(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/op_region_level_1' 
    \p \echo \echo 
    
EOF
