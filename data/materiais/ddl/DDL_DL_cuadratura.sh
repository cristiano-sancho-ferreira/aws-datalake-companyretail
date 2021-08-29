#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_cuadratura.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.cuadratura_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.cuadratura;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.cuadratura_flatfile(
  country_cd varchar(50), 
  country_desc varchar(50), 
  business_unit_id varchar(50), 
  business_unit_desc varchar(50), 
  source_id varchar(50), 
  source_status_cd varchar(50), 
  source_desc varchar(50), 
  object_id varchar(50), 
  object_desc varchar(50), 
  kpi_id varchar(50), 
  kpi_status_cd varchar(50), 
  kpi_desc varchar(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/cuadratura';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.cuadratura(
  country_cd varchar(50), 
  country_desc varchar(50), 
  business_unit_id varchar(50), 
  business_unit_desc varchar(50), 
  source_id varchar(50), 
  source_status_cd varchar(50), 
  source_desc varchar(50), 
  object_id varchar(50), 
  object_desc varchar(50), 
  kpi_id varchar(50), 
  kpi_status_cd varchar(50), 
  kpi_desc varchar(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/cuadratura' 
    \p \echo \echo 
    
EOF
