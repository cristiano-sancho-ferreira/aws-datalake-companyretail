#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_plan_metric.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.plan_metric_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.plan_metric;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.plan_metric_flatfile(
budget_metric_cd varchar (50) ,
budget_metric_desc varchar (255) ,
specific_budget_ind varchar (3))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/plan_metric';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.plan_metric(
budget_metric_cd varchar (50) ,
budget_metric_desc varchar (255) ,
specific_budget_ind varchar (3))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/plan_metric' 
    \p \echo \echo 
    
EOF
