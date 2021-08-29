#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_obsoles_rangos.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.obsoles_rangos_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.obsoles_rangos;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.obsoles_rangos_flatfile(
Dia_Inicial double precision,
Dia_Final double precision,
rango double precision,
nombre_rango VARCHAR(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/obsoles_rangos';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.obsoles_rangos(
Dia_Inicial double precision,
Dia_Final double precision,
rango double precision,
nombre_rango VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/obsoles_rangos' 
    \p \echo \echo 
    
EOF
