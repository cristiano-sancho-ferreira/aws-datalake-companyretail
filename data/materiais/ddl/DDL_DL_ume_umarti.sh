#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_ume_umarti.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.ume_umarti_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.ume_umarti;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.ume_umarti_flatfile (
articulo_cd VARCHAR(18),
umedida_cd VARCHAR(10),
codigo_barra_cd VARCHAR(18),
numerador_conv_umb double precision,
denominador_conv_umb double precision,
longitud double precision,
ancho double precision,
altura double precision,
lin_umedida_cd VARCHAR(10),
volumen double precision,
vol_umedida_cd VARCHAR(10),
peso_bruto double precision,
peso_umedida_cd VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/ume_umarti';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.ume_umarti (
articulo_cd VARCHAR(18),
umedida_cd VARCHAR(10),
codigo_barra_cd VARCHAR(18),
numerador_conv_umb double precision,
denominador_conv_umb double precision,
longitud double precision,
ancho double precision,
altura double precision,
lin_umedida_cd VARCHAR(10),
volumen double precision,
vol_umedida_cd VARCHAR(10),
peso_bruto double precision,
peso_umedida_cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/ume_umarti';
    \p \echo \echo 
    
EOF
