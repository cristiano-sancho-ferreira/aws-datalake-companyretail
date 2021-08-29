#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_centro_county.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.centro_county_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.centro_county;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.centro_county_flatfile(
  county_cd varchar(50), 
  county_name varchar(100), 
  territory_cd varchar(3), 
  msa_cd varchar(3))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/centro_county';
    \p \echo \echo 


 CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.centro_county(
  county_cd varchar(50), 
  county_name varchar(100), 
  territory_cd varchar(3), 
  msa_cd varchar(3))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/centro_county';
    \p \echo \echo 
    
EOF
