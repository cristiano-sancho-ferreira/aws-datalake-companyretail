#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_acu_clase.sh
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

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.acu_clase_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.acu_clase;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.acu_clase_flatfile(
operacion_cd VARCHAR(3),
clase_grupo VARCHAR(10),
clase_grupo_desc VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/acu_clase';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.acu_clase(
operacion_cd VARCHAR(3),
clase_grupo VARCHAR(10),
clase_grupo_desc VARCHAR(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/acu_clase' 
    \p \echo \echo 
    
EOF
