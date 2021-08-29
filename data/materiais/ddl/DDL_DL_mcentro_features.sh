#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_mcentro_features.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.mcentro_features_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.mcentro_features;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.mcentro_features_flatfile(
Location_Id varchar (20) ,
Country_Cd varchar (50) ,
Op_Geography_Level_1_Cd varchar (50) ,
Op_Region_Level_1_Cd varchar (50) ,
Group_Location_Level_1_Cd varchar (50) ,
Location_Manager_Cd varchar (50) ,
Latitude_Rate double precision,
Longitude_Rate double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/mcentro_features';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.mcentro_features(
Location_Id varchar (20) ,
Country_Cd varchar (50) ,
Op_Geography_Level_1_Cd varchar (50) ,
Op_Region_Level_1_Cd varchar (50) ,
Group_Location_Level_1_Cd varchar (50) ,
Location_Manager_Cd varchar (50) ,
Latitude_Rate double precision,
Longitude_Rate double precision)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/mcentro_features' 
    \p \echo \echo 
    
EOF
