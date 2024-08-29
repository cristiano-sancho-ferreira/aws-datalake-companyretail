#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_impu_mimp.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.impu_mimp_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.impu_mimp;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.impu_mimp_flatfile (
operacion_cd VARCHAR(1),
material_cd VARCHAR(18),
tipo_impuesto_cd VARCHAR(10),
tipo_alicuota_cd VARCHAR(10),
codigo_barra_venta VARCHAR(18))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/impu_mimp';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.impu_mimp (
operacion_cd VARCHAR(1),
material_cd VARCHAR(18),
tipo_impuesto_cd VARCHAR(10),
tipo_alicuota_cd VARCHAR(10),
codigo_barra_venta VARCHAR(18))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/impu_mimp';
    \p \echo \echo 
    
EOF
