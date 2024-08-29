#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_arti_artp.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.arti_artp_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.arti_artp;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.arti_artp_flatfile (
operacion_cd VARCHAR(1),
articulo_padre_cd VARCHAR(18),
fecha_novedad DATE,
borrado_logico VARCHAR(4),
merc_nivel4_cd VARCHAR(18),
articulo_desc VARCHAR(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/arti_artp';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.arti_artp (
operacion_cd VARCHAR(1),
articulo_padre_cd VARCHAR(18),
fecha_novedad VARCHAR(10),
borrado_logico VARCHAR(4),
merc_nivel4_cd VARCHAR(18),
articulo_desc VARCHAR(100))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/arti_artp';
    \p \echo \echo 
    
EOF
