#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_niv_niv0.sh
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
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.niv_niv0_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.niv_niv0;
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.niv_niv0_flatfile (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel0_cd VARCHAR(16),
fecha_novedad DATE,
merc_nivel0_desc VARCHAR(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/niv_niv0';
    \p \echo \echo

CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.niv_niv0 (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel0_cd VARCHAR(16),
fecha_novedad VARCHAR(10),
merc_nivel0_desc VARCHAR(100))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/niv_niv0';
    \p \echo \echo 
    
EOF
