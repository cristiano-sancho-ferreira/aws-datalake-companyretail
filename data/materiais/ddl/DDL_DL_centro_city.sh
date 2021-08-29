#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_centro_city.sh
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

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.centro_city_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.centro_city;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.centro_city_flatfile (
city_cd varchar(50),
city_name varchar(100),
territory_cd varchar(3),
county_cd varchar(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/centro_city';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.centro_city (
city_cd varchar(50),
city_name varchar(100),
territory_cd varchar(3),
county_cd varchar(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/centro_city';
    \p \echo \echo 
    
EOF
